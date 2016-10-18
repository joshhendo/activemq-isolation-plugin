package org.apache.activemq.isolation.lock;

import java.util.*;

public class CorrelationIdLocks {
    private String correlationId;

    private int semaphore;
    private Set<Lock> locks;
    private List<String> messages;
    private boolean used;

    public CorrelationIdLocks(String correlationId) {
        this.correlationId = correlationId;
        this.semaphore = 0;
        this.locks = new HashSet<Lock>();
        this.messages = Collections.synchronizedList(new ArrayList<String>());
        this.used = false;
    }

    public Set<Lock> getLocks() {
        return this.locks;
    }

    public List<String> getMessages() {
        return this.messages;
    }

    public boolean areLocksReleased() {
        if (!this.used) {
            return false;
        }

        return this.semaphore <= 0;
    }

    public boolean hasLock(String lockId) {
        for (Lock lock : this.locks) {
            if (lock.getLockId().equalsIgnoreCase(lockId)) {
                return true;
            }
        }

        return false;
    }

    public void removeLock(Lock lock) {
        this.locks.remove(lock);
    }

    public void addLock(Lock lock){
        if (hasLock(lock.getLockId())) {
            return;
        }

        this.locks.add(lock);
    }

    public synchronized void addMessage(String messageId, int count) {
        assert(messageId != null);

        if (areLocksReleased()) {
            // TODO: Throw exceptions
            return;
        }

        this.used = true;

        for (int i = 0; i < count; i++) {
            this.messages.add(messageId);
            this.semaphore += 1;
        }
    }

    public synchronized void acknoweldgeMessage(String messageId) {
        assert(messageId != null);

        if (!messages.contains(messageId)) {
            // TODO: Throw exceptions
            return;
        }

        for (int i = 0; i < this.messages.size(); i++){
            String currentMessage = this.messages.get(i);
            if (currentMessage.equals(messageId)) {
                this.messages.remove(i);
                break;
            }
        }

        this.semaphore -= 1;
    }
}
