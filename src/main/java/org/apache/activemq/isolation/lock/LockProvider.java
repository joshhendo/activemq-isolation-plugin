package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockProvider implements ILockProvider {

    private ConcurrentHashMap<String, String> messageIdToCorrelationId;
    private ConcurrentHashMap<String, CorrelationIdLocks> correlationIdToLocks;
    private ConcurrentHashMap<String, Lock> locks;

    public LockProvider() throws Exception {
        this.messageIdToCorrelationId = new ConcurrentHashMap<String, String>();
        this.correlationIdToLocks = new ConcurrentHashMap<String, CorrelationIdLocks>();

        this.locks = new ConcurrentHashMap<String, Lock>();
    }

    public synchronized boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys) {
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
                Lock obtainedLock = obtainLock(lockId);

                if (obtainedLock == null) {
                    success = false;
                    break;
                }

                obtainedLocks.add(obtainedLock);
                correlationIdLockEntry.addLock(obtainedLock);
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

        correlationIdLockEntry.addMessage(messageId);
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
        this.messageIdToCorrelationId.remove(messageId);

        if (correlationIdLockEntry.areLocksReleased()) {
            releaseLocksForCorrelationId(correlationIdLockEntry);
        }

        return true;
    }

    // Returns lock object when lock is successfully obtained, otherwise null
    private synchronized Lock obtainLock(String lockId) {
        if (!this.locks.containsKey(lockId)) {
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
}
