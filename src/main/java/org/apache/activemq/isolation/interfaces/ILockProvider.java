package org.apache.activemq.isolation.interfaces;


import java.util.HashMap;

public interface ILockProvider {
    boolean obtainLocksForMessage(String messageId, String correlationId, String messageName, HashMap<String, String> keys, int count) throws InterruptedException;
    boolean releaseLocksForMessage(String messageId);
}
