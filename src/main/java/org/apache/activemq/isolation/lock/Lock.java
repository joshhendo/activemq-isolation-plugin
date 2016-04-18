package org.apache.activemq.isolation.lock;

import java.util.Date;
import java.util.HashSet;

public class Lock {
    private Date created;
    private int countSemaphore;
    private HashSet<String> messageIds;
    private String correlationId;

    private boolean used;

    public Lock(String messageName) {
        this.created = new Date();
        this.countSemaphore = 0;
        this.messageIds = new HashSet<String>();
        this.correlationId = null;
        this.used = false;
    }

    public boolean isLockReleased() {
        if (!this.used) {
            return false;
        }

        return countSemaphore <= 0;
    }

    public boolean compareCorrelationId(String messageId, String correlationId) {
        assert(messageId != null);
        if (correlationId == null) {
            correlationId = messageId;
        }

       return this.correlationId.equals(correlationId);
    }

    public synchronized void addMessage(String messageId, String correlationId) {
        assert(messageId != null);

        if (isLockReleased()) {
            // TODO: Throw exception; once a lock is released new messages can't be added
            return;
        }

        if (!this.used) {
            // If the correlation ID is null, the first message ID is treated as the correlation ID
            if (correlationId == null) {
                correlationId = messageId;
            }

            this.correlationId = correlationId;
        }

        if (!correlationId.equals(this.correlationId)) {
            // TODO: Throw exception, can't add a message to the wrong correlation ID
            return;
        }

        this.used = true;

        messageIds.add(messageId);
        countSemaphore += 1;
    }

    public synchronized void ackMessage(String messageId, String correlationId) {
        assert(messageId != null);
        if (correlationId == null) {
            correlationId = messageId;
        }

        if (!messageIds.contains(messageId)) {
            // TODO: Throw exception
            return;
        }

        if (!correlationId.equals(this.correlationId)) {
            // TODO: Throw exception
            return;
        }

        countSemaphore -= 1;
    }
}
