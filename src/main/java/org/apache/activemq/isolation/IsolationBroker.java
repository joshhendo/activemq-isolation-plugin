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
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.json.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IsolationBroker extends BrokerFilter {

	ILockProvider lockProvider;
	SchemaFile definitions;

	public IsolationBroker(Broker next, ILockProvider lockProvider, String definitionFile) throws IOException {
		super(next);
		this.lockProvider = lockProvider;
		this.definitions = readInDefinitionFile(definitionFile);
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
		processMessage(producerExchange, messageSend);
		super.send(producerExchange, messageSend);
	}

	public void processMessage(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception, NoLockException, NoKeyException {
		String messageId = messageSend.getMessageId().toString();
		String correlationId = messageSend.getCorrelationId();

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
		boolean lockObtained = this.lockProvider.obtainLocksForMessage(messageId, correlationId, messageName, keys);
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
}
