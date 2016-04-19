package org.apache.activemq.isolation.lock;

import java.util.Date;
import java.util.HashSet;

public class Lock {
    private String lockId;
    private Date created;

    public Lock(String lockId) {
        this.lockId = lockId;
        this.created = new Date();
    }

    public String getLockId() {
        return this.lockId;
    }

    public Date getCreatedDate() {
        return this.created;
    }
}
