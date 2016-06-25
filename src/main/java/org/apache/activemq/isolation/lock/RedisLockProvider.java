package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;
import org.redisson.*;
import org.redisson.core.RMap;
import org.redisson.core.RQueue;

import java.util.HashMap;

public class RedisLockProvider extends LockProvider{

    public RedissonClient redisson;

    public RedisLockProvider() throws Exception {
        redisson = Redisson.create();
        messageIdToCorrelationId = redisson.getMap("MESSAGE_ID_TO_CORRELATION_ID");
        correlationIdToLocks = redisson.getMap("CORRELATION_ID_TO_LOCKS");
        locks = redisson.getMap("LOCKS");
    }

}
