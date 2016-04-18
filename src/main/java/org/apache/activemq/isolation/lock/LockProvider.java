package org.apache.activemq.isolation.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LockProvider {

    private HashMap<String, Lock> locks;

    public LockProvider() {
        this.locks = new HashMap<String, Lock>();
    }

    public synchronized boolean obtainLocks() {
        return false;
    }

    // Returns true when lock is successfully obtained
    private synchronized boolean obtainLock(String messageId, String correlationId, String messageName, HashMap<String, String> keys) {
        String key = messageName + ":" + "";

        Lock currentLock = locks.get(key);
        if (currentLock == null) {
            // Add a lock for this key
            currentLock = new Lock(messageName);
            currentLock.addMessage(messageId, correlationId);
            return true;
        } else {
            // A lock is already held on this key; check if it's the same correlation ID
            if (currentLock.compareCorrelationId(messageId, correlationId)) {
                currentLock.addMessage(messageId, correlationId);
            }

            return false;
        }
    }

    public synchronized void releaseLock(String messageId, String correlationId) {

    }
}
