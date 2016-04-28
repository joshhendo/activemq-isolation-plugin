package org.apache.activemq.isolation.schema.json;

public class Message {
    public String type;
    public String[] keys;
    public Lock[] locks;
}
