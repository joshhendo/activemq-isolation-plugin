package org.apache.activemq.isolation.lock;

import java.util.Calendar;
import java.util.Date;

public class Lock {
    private String lockId;
    private Date created;

    public Lock(String lockId) {
        this.lockId = lockId;
        this.created = Calendar.getInstance().getTime();
    }

    public String getLockId() {
        return this.lockId;
    }

    public Date getCreatedDate() {
        return this.created;
    }
}
