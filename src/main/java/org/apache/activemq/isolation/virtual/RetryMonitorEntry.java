package org.apache.activemq.isolation.virtual;

import org.apache.activemq.command.Message;
import org.apache.activemq.broker.ProducerBrokerExchange;

public class RetryMonitorEntry {
    private ProducerBrokerExchange producerExchange;
    private Message messageSend;

    public RetryMonitorEntry(ProducerBrokerExchange producerExchange, Message messageSend) {
        this.producerExchange = producerExchange;
        this.messageSend = messageSend;
    }

    public ProducerBrokerExchange getProducerExchange() {
        return producerExchange;
    }

    public Message getMessage() {
        return messageSend;
    }
}
