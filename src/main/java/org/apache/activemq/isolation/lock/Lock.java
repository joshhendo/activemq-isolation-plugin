package org.apache.activemq.isolation.lock;

import java.util.Calendar;
import java.util.Date;

public class Lock {
    private String lockId;
    private String guid;
    private Date created;

    public Lock(String lockId) {
        this.lockId = lockId;
        this.guid = java.util.UUID.randomUUID().toString();
        this.created = Calendar.getInstance().getTime();
    }

    public String getLockId() {
        return this.lockId;
    }

    public String getGuid() {
        return this.guid;
    }

    public Date getCreatedDate() {
        return this.created;
    }
}
