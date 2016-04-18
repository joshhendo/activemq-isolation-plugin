package org.apache.activemq.isolation;

import org.apache.activemq.isolation.NoLockException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;

public class IsolationBroker extends BrokerFilter {

	List<String> messagesToInspect;
	
	public IsolationBroker(Broker next, List<String> messagesToInspect) {
		super(next);
		this.messagesToInspect = messagesToInspect;
	}

	public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
		super.addConnection(context, info);
	}	

	public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
		System.out.println("Acknoweldge!!");
		super.acknowledge(consumerExchange, ack);
	}

	public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception, NoLockException {
		byte[] data = messageSend.getContent().data;
		String content = new String(data, 0, data.length, "ASCII");

		if (content.contains("test")) {
			throw new NoLockException("Test lock");
		}

		super.send(producerExchange, messageSend);
	}
	
}
