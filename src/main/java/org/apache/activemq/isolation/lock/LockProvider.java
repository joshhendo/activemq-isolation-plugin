package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockProvider implements ILockProvider {

    public static boolean VirtualQueueEnabled = false;

    private ConcurrentHashMap<String, String> messageIdToCorrelationId;
    private ConcurrentHashMap<String, CorrelationIdLocks> correlationIdToLocks;
    private ConcurrentHashMap<String, Lock> locks;
    private ConcurrentLinkedQueue<VirtualQueueEntry> virtualQueue;

    public LockProvider() throws Exception {
        this.messageIdToCorrelationId = new ConcurrentHashMap<String, String>();
        this.correlationIdToLocks = new ConcurrentHashMap<String, CorrelationIdLocks>();

        this.locks = new ConcurrentHashMap<String, Lock>();
        this.virtualQueue = new ConcurrentLinkedQueue<VirtualQueueEntry>();
    }

    public synchronized boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys, int count) {
        assert(messageId != null);
        if (correlationId == null) {
            correlationId = messageId;
        }

        // Map message to correlation ID
        messageIdToCorrelationId.put(messageId, correlationId);

        // Get existing locks for correlation ID
        CorrelationIdLocks correlationIdLockEntry = this.correlationIdToLocks.get(correlationId);
        if (correlationIdLockEntry == null) {
            correlationIdLockEntry = new CorrelationIdLocks(correlationId);
            this.correlationIdToLocks.put(correlationId, correlationIdLockEntry);
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
                    if (LockProvider.VirtualQueueEnabled) {
                        VirtualQueueEntry virtualQueueEntry = new VirtualQueueEntry(messageName, entry.getKey(), entry.getValue(), messageId);
                        this.virtualQueue.add(virtualQueueEntry);
                    }

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
                this.locks.remove(lock.getLockId());
                correlationIdLockEntry.removeLock(lock);
            }

            return false;
        }

        correlationIdLockEntry.addMessage(messageId, count);
        return true;
    }

    public synchronized boolean releaseLocksForMessage(String messageId) {
        String correlationId = messageIdToCorrelationId.get(messageId);

        if (correlationId == null) {
            // TODO: Throw excepton
            return false;
        }

        CorrelationIdLocks correlationIdLockEntry = correlationIdToLocks.get(correlationId);
        if (correlationIdLockEntry == null) {
            // TODO: Throw exceptions
            return false;
        }

        correlationIdLockEntry.acknoweldgeMessage(messageId);

        boolean areThereOtherInstancesOfTheSameMessage = false;
        for (String currentMessageId : correlationIdLockEntry.getMessages()) {
            if (messageId.equals(currentMessageId)) {
                areThereOtherInstancesOfTheSameMessage = true;
                break;
            }
        }

        if (!areThereOtherInstancesOfTheSameMessage) {
            this.messageIdToCorrelationId.remove(messageId);
        }

        if (correlationIdLockEntry.areLocksReleased()) {
            releaseLocksForCorrelationId(correlationIdLockEntry);
        }

        return true;
    }

    // Returns lock object when lock is successfully obtained, otherwise null
    private synchronized Lock obtainLock(String messageType, String keyName, String keyValue, String messageId) {
        String lockId = messageType + ":" + keyName + ":" + keyValue;

        // Ensure that this lock isn't already held by another process
        if (!this.locks.containsKey(lockId)) {
            // Ensure that this lock isn't in a virtual queue
            // TODO: Add a timeout for virtual queue entries

            if (LockProvider.VirtualQueueEnabled) {
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
            }

            Lock newLock = new Lock(lockId);

            // Attempt to obtain the lock
            locks.put(lockId, newLock);

            // Optimistic concurrency, check that it wasn't another process
            // that added the lock
            if (locks.get(lockId) == newLock) {
                return newLock;
            }
        }

        return null;
    }

    private synchronized void releaseLocksForCorrelationId(CorrelationIdLocks correlationIdLockEntry) {
        assert(correlationIdLockEntry != null);

        // Free all locks
        for (Lock lock : correlationIdLockEntry.getLocks()) {
            this.locks.remove(lock.getLockId());
        }

        // Remove correlation Id entry
        this.correlationIdToLocks.remove(correlationIdLockEntry);
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
