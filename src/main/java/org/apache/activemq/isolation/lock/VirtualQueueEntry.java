package org.apache.activemq.isolation.lock;

import java.util.Date;

public class VirtualQueueEntry {
    private String messageType;
    private String keyName;
    private String keyValue;
    private Date created;

    public VirtualQueueEntry(String messageType, String keyName, String keyValue, String messageId) {
        this.created = new Date();
    }

    public String getMessageType() {
        return this.messageType;
    }

    public String getKeyName() {
        return this.keyName;
    }

    public String getKeyValue() {
        return this.keyValue;
    }

    public String getLockId() {
        String lockId = this.messageType;
        if (this.keyName != null && this.keyValue != null) {
            lockId += ":" + this.keyName + ":" + this.keyValue;
        }

        return lockId;
    }

    public boolean isCoarseLock() {
        if (this.keyName == null && this.keyValue == null) {
            return true;
        }
        return false;
    }

    public boolean matches(String messageType, String keyName, String keyValue) {
        return (this.messageType.equals(messageType) && this.keyName.equals(keyName) && this.keyValue.equals(keyValue));
    }

    public Date getCreatedDate() {
        return this.created;
    }
}
