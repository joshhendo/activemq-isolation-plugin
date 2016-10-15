package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;
import org.redisson.*;
import org.redisson.core.RBucket;
import org.redisson.core.RList;
import org.redisson.core.RMap;
import org.redisson.core.RQueue;

import java.util.HashMap;

public class RedisLockProvider extends LockProvider{

    public RedissonClient redisson;

    public RedisLockProvider() throws Exception {
        redisson = Redisson.create();
        messageIdToCorrelationId = redisson.getMap("MESSAGE_ID_TO_CORRELATION_ID");
        //correlationIdToLocks = redisson.getMap("CORRELATION_ID_TO_LOCKS");
        locks = redisson.getMap("LOCKS");
    }

    @Override
    protected String getMessageIdToCorrelationId(String messageId) {
        return super.getMessageIdToCorrelationId(messageId);
    }

    @Override
    protected void removeMessageIdToCorrelationId(String messageId) {
        super.removeMessageIdToCorrelationId(messageId);
    }

    @Override
    protected void addMessageIdToCorrelationId(String messageId, String correlationId) {
        super.addMessageIdToCorrelationId(messageId, correlationId);
    }

    private RBucket<CorrelationIdLocks> getListForCorrelationIdToLocks (String correlationId) {
        return redisson.getBucket("CORRELATION_ID_TO_LOCKS_" + correlationId);
    }

    @Override
    protected CorrelationIdLocks getCorrelationIdToLocks(String correlationId) {
        RBucket<CorrelationIdLocks> distributedCorrelationIdToLocks = getListForCorrelationIdToLocks(correlationId);
        return distributedCorrelationIdToLocks.get();

        /*CorrelationIdLocks correlationIdLocks = new CorrelationIdLocks(correlationId);
        for (Lock lock : distributedCorrelationIdToLocks) {
            correlationIdLocks.addLock(lock);
        }*

        return correlationIdLocks; */
    }

    @Override
    protected void setCorrelationIdToLocks(String correlationId, CorrelationIdLocks correlationIdLockEntry) {
        RBucket<CorrelationIdLocks> distributedCorrelationIdToLocks = getListForCorrelationIdToLocks(correlationId);
        distributedCorrelationIdToLocks.set(correlationIdLockEntry);
    }

    @Override
    protected void removeCorrelationIdToLocks(String correlationId, CorrelationIdLocks correlationIdLocksEntry) {
        RBucket<CorrelationIdLocks> distributedCorrelationIdToLocks = getListForCorrelationIdToLocks(correlationId);
        distributedCorrelationIdToLocks.set(null);
    }

    @Override
    protected Boolean existsLock(String lockId) {
        return super.existsLock(lockId);
    }

    /*@Override
    protected Lock getLock(String lockId) {
        return super.getLock(lockId);
    }*/

    @Override
    protected void setLock(String lockId, Lock lock) {
        super.setLock(lockId, lock);
    }

    @Override
    protected void removeLock(String lockId) {
        super.removeLock(lockId);
    }
}
