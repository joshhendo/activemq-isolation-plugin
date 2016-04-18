package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;

import java.util.*;

public class LockProvider implements ILockProvider {
    private HashMap<String, Lock> locks;

    public LockProvider() throws Exception {
        this.locks = new HashMap<String, Lock>();
    }

    public synchronized boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys) {
        List<ObtainLockResult> obtainedLocks = new ArrayList<ObtainLockResult>();
        boolean success = true;

        // Iterate over the locks we need to obtain
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            ObtainLockResult obtainLockResult = obtainLock(messageId, correlationId, messageName, entry);

            if (!obtainLockResult.getObtained()) {
                success = false;
                break;
            }

            obtainedLocks.add(obtainLockResult);
        }

        // If unsuccessful, we need to release all the ones that were newly obtained
        // AND remove the new message from ones with the semaphore incremented
        if (!success) {
            for (ObtainLockResult obtainLockResult : obtainedLocks) {
                releaseLock(messageId, correlationId, obtainLockResult.getKeyName());
            }

            return false;
        }

        return true;
    }

    public synchronized boolean releaseLocksForMessage(String messageId, String correlationId, String messageName) {
        return false;
    }

    // Returns true when lock is successfully obtained
    private synchronized ObtainLockResult obtainLock(String messageId, String correlationId, String messageName, Map.Entry<String, String> key) {
        String keyName = messageName + ":" + key.getKey() + ":" + key.getValue();

        Lock currentLock = locks.get(keyName);
        if (currentLock == null) {
            currentLock = new Lock(messageName);
            currentLock.addMessage(messageId, correlationId);
            locks.put(keyName, currentLock);

            return new ObtainLockResult(keyName, true, false);

        } else if (currentLock.compareCorrelationId(messageId, correlationId)) {
            currentLock.addMessage(messageId, correlationId);
            return new ObtainLockResult(keyName, true, true);
        }

        return new ObtainLockResult(keyName, false, false);
    }

    private synchronized void releaseLock(String messageId, String correlationId, String keyName) {
        Lock currentLock = locks.get(keyName);
        if (currentLock != null) {
            currentLock.ackMessage(messageId, correlationId);
        }
    }
}
