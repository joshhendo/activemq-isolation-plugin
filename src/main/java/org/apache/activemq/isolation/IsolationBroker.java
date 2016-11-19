package org.apache.activemq.isolation;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.isolation.exceptions.NoKeyException;
import org.apache.activemq.isolation.exceptions.NoLockException;
import org.apache.activemq.isolation.interfaces.ILockProvider;
import org.apache.activemq.isolation.schema.SchemaFile;
import org.apache.activemq.isolation.virtual.RetryQueue;
import org.apache.activemq.isolation.virtual.RetryMonitorEntry;
import org.json.*;

import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.command.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public class IsolationBroker extends BrokerFilter {
    public final String ERROR_HANDLING_VIRTUAL = "VirtualQueue";
    public final String ERROR_HANDLING_IMMEDIATE = "ImmediateFailure";

	ILockProvider lockProvider;
	SchemaFile definitions;
	AdvisoryBroker advisoryBroker;
    Thread virtualQueueMonitorThread;
    String errorHandling;

	public IsolationBroker(final Broker next, ILockProvider lockProvider, String definitionFile, String errorHandling) throws Exception {
		super(next);
		this.lockProvider = lockProvider;
		this.definitions = readInDefinitionFile(definitionFile);

        if (!errorHandling.equals(ERROR_HANDLING_VIRTUAL) && !errorHandling.equals(ERROR_HANDLING_IMMEDIATE)) {
            throw new ExceptionInInitializerError("errorHandling value in config needs to be set to 'VirtualQueue' or 'ImmediateFailure'");
        }

        this.errorHandling = errorHandling;
    }

	private SchemaFile readInDefinitionFile(String definitionFile) throws IOException {
		return SchemaFile.readFile(definitionFile);
	}

	@Override
	public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
		processAcknowledge(consumerExchange, ack);
		super.acknowledge(consumerExchange, ack);
	}

	@Override
	public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception, NoLockException {
		try {
		    processMessage(producerExchange, messageSend);

            // It will only get to here if a lock is obtained
            // If the lock is not obtained, the catch statement will:
            //   - Re-throw the exception if set to ImmediateFailure
            //   - Add to a queue to be retried at a later time if set to VirtualQueue
            super.send(producerExchange, messageSend);
        } catch (NoLockException exception) {
            // if it's immediate failure, throw the exception
            if (this.errorHandling.equals(ERROR_HANDLING_IMMEDIATE)) {
                throw exception;
            }
            // If it's virtual queue, don't filter down the exception and add to retry queue
            else if (this.errorHandling.equals(ERROR_HANDLING_VIRTUAL)) {
                RetryQueue.addToQueue(new RetryMonitorEntry(producerExchange, messageSend));
            }
        }
	}

	public void processMessage(final ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception, NoLockException, NoKeyException {
		if (this.advisoryBroker == null) {
			this.advisoryBroker = (AdvisoryBroker) next.getBrokerService().getBroker().getAdaptor(AdvisoryBroker.class);
		}

		if (this.virtualQueueMonitorThread == null) {
            virtualQueueMonitorThread = new Thread() {
                public void run() {
                    try {
                        RetryQueue.Monitor(next.getBrokerService().getBroker());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            virtualQueueMonitorThread.start();
        }

		String messageId = messageSend.getMessageId().toString();
		String correlationId = messageSend.getCorrelationId();

		int numberOfDestinations = 1;
		if (messageSend.getDestination() instanceof ActiveMQTopic) {
			numberOfDestinations = getConsumersForDestination(messageSend.getDestination(), this.advisoryBroker.getAdvisoryConsumers());
		}
        byte[] data = messageSend.getContent().data;
		String content = new String(data, 0, data.length, "ASCII").trim();
		int i = content.indexOf("{");
		content = content.substring(i);

		// Try and parse JSON
		JSONObject jsonObject = new JSONObject(content);
		String messageName = jsonObject.getString("message");

		// Read in the required keys from the definition file
		String[] requiredKeys = this.definitions.GetRequiredKeys(messageName);
		if (requiredKeys == null || requiredKeys.length == 0) {
			return;
		}

		// TODO: Extract the relevant keys that need to be locked on (if needed)
		HashMap<String, String> keys = new HashMap<String, String>();
		for (String requiredKey : requiredKeys) {
			if (!jsonObject.has(requiredKey)) {
				throw new NoKeyException("Missing key '" + requiredKey + "'");
			}

			String keyValue = jsonObject.getString(requiredKey);
			keys.put(requiredKey, keyValue);
		}

		// Try and obtain lock
		System.out.println("Obtaining lock with messageId=" + messageId + " and correlationId=" + correlationId);
		boolean lockObtained = this.lockProvider.obtainLocksForMessage(messageId, correlationId, messageName, keys, numberOfDestinations);
		if (!lockObtained) {
			throw new NoLockException();
		}
	}

	public void processAcknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        String messageId = ack.getLastMessageId().toString();
		System.out.println("Ack message with messageId=" + messageId);
		// System.out.println(ReflectionToStringBuilder.toString(ack, new RecursiveToStringStyle()));
		this.lockProvider.releaseLocksForMessage(messageId);
	}

	private int getConsumersForDestination(ActiveMQDestination destination, Collection<ConsumerInfo> consumers) {
		int count = 0;

		for (ConsumerInfo consumer : consumers) {
			if (consumer.getDestination() instanceof ActiveMQTopic) {
				if (consumer.getDestination().getPhysicalName().equals(destination.getPhysicalName())) {
					count ++;
				}
			}
		}

		return count;
	}

}
