package org.apache.activemq.isolation.lock;

import org.apache.activemq.isolation.interfaces.ILockProvider;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisLockProvider implements ILockProvider {

    public RedissonClient redissonClient;

    private String MESSAGE_ID_TO_CORRELATION_ID = "messageIdToCorrelationId";
    private String CORRELATION_ID_TO_LOCKS = "correlationIdToLocks";
    private String CORRELATION_ID_TO_MESSAGES = "correlationIdToMessages";
    private String CORRELATION_ID_TO_COUNTER = "correlationIdToCounter";

    public RedisLockProvider() throws Exception {
        redissonClient = Redisson.create();
    }

    public boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys, int count) throws InterruptedException {
        assert(messageId != null);
        if (correlationId == null) {
            correlationId = messageId;
        }

        redissonClient.getBucket(MESSAGE_ID_TO_CORRELATION_ID + ":" + messageId).set(correlationId);

        boolean isNewCorrelationId = false;
        RSet<String> correlationIdToLocks = redissonClient.getSet(CORRELATION_ID_TO_LOCKS + ":" + correlationId);
        if (correlationIdToLocks.isEmpty()) {
            isNewCorrelationId = true;
        }

        Set<String> obtainedLocks = new HashSet<String>();
        boolean success = true;

        for (Map.Entry<String, String> entry : keys.entrySet()) {
            String lockId = messageName + ":" + entry.getKey() + ":" + entry.getValue();

            if (!correlationIdToLocks.contains(lockId)) {
                boolean obtainedLock = obtainLock(messageName, entry.getKey(), entry.getValue(), messageId);
                if (!obtainedLock) {
                    redissonClient.getBucket(MESSAGE_ID_TO_CORRELATION_ID + ":" + messageId).delete();
                    if (isNewCorrelationId) {
                        redissonClient.getSet(CORRELATION_ID_TO_LOCKS + ":" + correlationId).delete();
                    }

                    success = false;
                    break;
                }
                else {
                    correlationIdToLocks.add(lockId);
                }
            }
        }

        if (!success) {
            for (String lock : obtainedLocks) {
                correlationIdToLocks.remove(lock);
                redissonClient.getBucket(lock).delete();
            }

            return false;
        }

    for (int i = 0; i < count; i++) {
            redissonClient.getSet(CORRELATION_ID_TO_MESSAGES + ":" + correlationId).add(messageId);
            redissonClient.getAtomicLong(CORRELATION_ID_TO_COUNTER + ":" + correlationId).incrementAndGet();
        }


        return true;
    }

    private synchronized boolean obtainLock(String messageType, String keyName, String keyValue, String messageId) throws InterruptedException {
        String lockId = messageType + ":" + keyName + ":" + keyValue;
        String guid = java.util.UUID.randomUUID().toString();

        RLock verticalLock = redissonClient.getLock("lock:" + keyName);
        RLock lock = redissonClient.getLock("lock:" + lockId);
        if (verticalLock.tryLock(1, 1, TimeUnit.SECONDS) && lock.tryLock(1, 1, TimeUnit.SECONDS)) {
            if (redissonClient.getBucket(lockId).get() == null) {
                redissonClient.getBucket(lockId).set(guid);

                // ensure it was set properly
                if (redissonClient.getBucket(lockId).get().equals(guid)) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean releaseLocksForMessage(String messageId) {
        String correlationId = (String) redissonClient.getBucket(MESSAGE_ID_TO_CORRELATION_ID + ":" + messageId).get();
        if (correlationId == null) {
            // TODO: Throw exception
            return false;
        }

        RSet<String> correlationIdToLocks = redissonClient.getSet(CORRELATION_ID_TO_LOCKS + ":" + correlationId);
        if (correlationIdToLocks == null || correlationIdToLocks.isEmpty()) {
            // TODO: Throw exception
            return false;
        }

        // Acknoweldge message
        redissonClient.getSet(CORRELATION_ID_TO_MESSAGES + ":" + correlationId).remove(messageId);
        redissonClient.getAtomicLong(CORRELATION_ID_TO_COUNTER + ":" + correlationId).decrementAndGet();

        boolean areThereOtherInstancesOfTheSameMessage = false;
        if (redissonClient.getSet(CORRELATION_ID_TO_MESSAGES + ":" + correlationId).contains(messageId)) {
            areThereOtherInstancesOfTheSameMessage = true;
        }

        if (!areThereOtherInstancesOfTheSameMessage) {
            redissonClient.getBucket(MESSAGE_ID_TO_CORRELATION_ID + ":" + messageId).delete();
        }

        // Are locks released?
        if (redissonClient.getAtomicLong(CORRELATION_ID_TO_COUNTER + ":" + correlationId).get() <= 0) {
            // Remove all the locks associated with the correlation id
            Set<Object> locksHeldByCorrelationId = redissonClient.getSet(CORRELATION_ID_TO_LOCKS + ":" + correlationId).readAll();
            for (Object lockHeldByCorrelationId : locksHeldByCorrelationId) {
                redissonClient.getBucket((String) lockHeldByCorrelationId).delete();
            }

            redissonClient.getSet(CORRELATION_ID_TO_LOCKS + ":" + correlationId).delete();
        }

        return true;
    }
}
