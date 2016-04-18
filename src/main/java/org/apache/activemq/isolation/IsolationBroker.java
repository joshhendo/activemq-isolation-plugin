package org.apache.activemq.isolation;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.isolation.exception.NoLockException;
import org.apache.activemq.isolation.interfaces.ILockProvider;
import org.apache.activemq.isolation.lock.LockProvider;

import java.util.HashMap;
import java.util.List;

public class IsolationBroker extends BrokerFilter {

	ILockProvider lockProvider;
	List<String> messagesToInspect;
	
	public IsolationBroker(Broker next, ILockProvider lockProvider, List<String> messagesToInspect) {
		super(next);
		this.lockProvider = lockProvider;
		this.messagesToInspect = messagesToInspect;
	}

	@Override
	public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
		processAcknowledge(consumerExchange, ack);
		super.acknowledge(consumerExchange, ack);
	}

	@Override
	public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception, NoLockException {
		processMessage(producerExchange, messageSend);
		super.send(producerExchange, messageSend);
	}

	public void processMessage(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception, NoLockException {
		String messageId = messageSend.getMessageId().toString();
		String correlationId = messageSend.getCorrelationId();

        /*byte[] data = messageSend.getContent().data;
		String content = new String(data, 0, data.length, "ASCII");*/

		// TODO: Extract actual message name
		String messageName = "addUser";

		// TODO: Extract the relevant keys that need to be locked on (if needed)
		HashMap<String, String> keys = new HashMap<String, String>();
		keys.put("userid", "12345678");

		// Try and obtain lock
		boolean lockObtained = this.lockProvider.obtainLocksForMessage(messageId, correlationId, messageName, keys);
		if (!lockObtained) {
			throw new NoLockException();
		}
	}

	public void processAcknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
		System.out.println("ack");
	}

}
