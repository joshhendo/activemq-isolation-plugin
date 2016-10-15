package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class LockProvider implements ILockProvider {

    public ConcurrentMap<String, String> messageIdToCorrelationId;
    public ConcurrentMap<String, CorrelationIdLocks> correlationIdToLocks;
    public ConcurrentMap<String, Lock> locks;
    public ConcurrentLinkedQueue<VirtualQueueEntry> virtualQueue;

    // Functions to override
    protected String getMessageIdToCorrelationId(String messageId) {
        return this.messageIdToCorrelationId.get(messageId);
    }

    protected void removeMessageIdToCorrelationId (String messageId) {
        this.messageIdToCorrelationId.remove(messageId);
    }

    protected void addMessageIdToCorrelationId (String messageId, String correlationId) {
        this.messageIdToCorrelationId.put(messageId, correlationId);
    }

    protected CorrelationIdLocks getCorrelationIdToLocks(String correlationId) {
        return this.correlationIdToLocks.get(correlationId);
    }

    protected void setCorrelationIdToLocks(String correlationId, CorrelationIdLocks correlationIdLockEntry) {
        this.correlationIdToLocks.put(correlationId, correlationIdLockEntry);
    }

    protected void removeCorrelationIdToLocks(String correlationId, CorrelationIdLocks correlationIdLocksEntry) {
        this.correlationIdToLocks.remove(correlationIdLocksEntry);
    }

    protected Boolean existsLock(String lockId){
        return this.locks.containsKey(lockId);
    }

    protected Lock getLock(String lockId) {
        return this.locks.get(lockId);
    }

    protected void setLock(String lockId, Lock lock) {
        this.locks.put(lockId, lock);
    }

    protected void removeLock(String lockId) {
        this.locks.remove(lockId);
    }

    // End of things to override

    public LockProvider() throws Exception {
        this.messageIdToCorrelationId = new ConcurrentHashMap<String, String>();
        this.correlationIdToLocks = new ConcurrentHashMap<String, CorrelationIdLocks>();

        this.locks = new ConcurrentHashMap<String, Lock>();
        this.virtualQueue = new ConcurrentLinkedQueue<VirtualQueueEntry>();
    }

    public synchronized boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys) {
        assert(messageId != null);
        if (correlationId == null) {
            correlationId = messageId;
        }

        // Map message to correlation ID
        addMessageIdToCorrelationId(messageId, correlationId);

        // Get existing locks for correlation ID
        CorrelationIdLocks correlationIdLockEntry = getCorrelationIdToLocks(correlationId);
        if (correlationIdLockEntry == null) {
            correlationIdLockEntry = new CorrelationIdLocks(correlationId);
            setCorrelationIdToLocks(correlationId, correlationIdLockEntry);
        }

        Set<Lock> obtainedLocks = new HashSet<Lock>();
        boolean success = true;

        // Iterate and obtain NEW locks
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            String lockId = messageName + ":" + entry.getKey() + ":" + entry.getValue();

            // Only attempt to get a lock if it's not already held
            if (!correlationIdLockEntry.hasLock(lockId)) {
                Lock obtainedLock = obtainLock(messageName, entry.getKey(), entry.getValue(), messageId);

                // Was unable to obtain lock; add it to the virtual queue
                if (obtainedLock == null) {
                    VirtualQueueEntry virtualQueueEntry = new VirtualQueueEntry(messageName, entry.getKey(), entry.getValue(), messageId);
                    this.virtualQueue.add(virtualQueueEntry);

                    // We will set success to false
                    success = false;
                } else {
                    obtainedLocks.add(obtainedLock);
                    correlationIdLockEntry.addLock(obtainedLock);
                }
            }
        }

        // If unsuccessful, we need to release all the ones that were newly obtained
        if (!success) {
            for (Lock lock : obtainedLocks) {
                removeLock(lock.getLockId());
                correlationIdLockEntry.removeLock(lock);
            }

            return false;
        }

        correlationIdLockEntry.addMessage(messageId);
        return true;
    }

    public synchronized boolean releaseLocksForMessage(String messageId) {
        String correlationId = getMessageIdToCorrelationId(messageId);

        if (correlationId == null) {
            // TODO: Throw excepton
            return false;
        }

        CorrelationIdLocks correlationIdLockEntry = getCorrelationIdToLocks(correlationId);
        if (correlationIdLockEntry == null) {
            // TODO: Throw exceptions
            return false;
        }

        correlationIdLockEntry.acknoweldgeMessage(messageId);
        removeMessageIdToCorrelationId(messageId);

        if (correlationIdLockEntry.areLocksReleased()) {
            releaseLocksForCorrelationId(correlationId, correlationIdLockEntry);
        }

        return true;
    }

    // Returns lock object when lock is successfully obtained, otherwise null
    private synchronized Lock obtainLock(String messageType, String keyName, String keyValue, String messageId) {
        String lockId = messageType + ":" + keyName + ":" + keyValue;

        // Ensure that this lock isn't already held by another process
        if (!existsLock(lockId)) {
            // Ensure that this lock isn't in a virtual queue
            // TODO: Add a timeout for virtual queue entries
            VirtualQueueEntry lastVirtualQueueEntry = findLastMessageInVirtualQueue(messageType, keyName, keyValue);
            if (lastVirtualQueueEntry != null) {
                // If it isn't for this message ID, then we can't obtain the lock.
                // If it is for this message ID, then we can remove it from the queue.
                if (lastVirtualQueueEntry.getMessageId().equals(messageId)) {
                    this.virtualQueue.remove(lastVirtualQueueEntry);
                } else {
                    return null;
                }
            }

            Lock newLock = new Lock(lockId);

            // Attempt to obtain the lock
            setLock(lockId, newLock);

            Lock gotLock = getLock(lockId);

            // Optimistic concurrency, check that it wasn't another process
            // that added the lock
            if (gotLock != null && gotLock.getGuid().equals(newLock.getGuid())) {
                return newLock;
            }
        }

        return null;
    }

    private synchronized void releaseLocksForCorrelationId(String correlationId, CorrelationIdLocks correlationIdLockEntry) {
        assert(correlationIdLockEntry != null);

        // Free all locks
        for (Lock lock : correlationIdLockEntry.getLocks()) {
            removeLock(lock.getLockId());
        }

        // Remove correlation Id entry
        removeCorrelationIdToLocks(correlationId, correlationIdLockEntry);
    }

    // This function is for proof of concept implementation only.
    // It should be redesigned to a more robust and scalable implementation before being used
    // in a production environment.
    private synchronized VirtualQueueEntry findLastMessageInVirtualQueue(String messageType, String keyName, String keyValue) {
        Object[] virtualQueueArray = this.virtualQueue.toArray();

        VirtualQueueEntry lastVirtualQueueEntry = null;
        for (int i = virtualQueueArray.length-1; i >= 0; i--) {
            VirtualQueueEntry virtualQueueEntry = (VirtualQueueEntry) virtualQueueArray[i];

            if (virtualQueueEntry.isCoarseLock() && virtualQueueEntry.getMessageType().equals(messageType)) {
                lastVirtualQueueEntry = virtualQueueEntry;
            } else if (virtualQueueEntry.matches(messageType, keyName, keyValue)) {
                lastVirtualQueueEntry = virtualQueueEntry;
            }
        }

        return lastVirtualQueueEntry;
    }
}
