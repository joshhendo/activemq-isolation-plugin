package org.apache.activemq.isolation;

import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

public class IsolationPlugin implements BrokerPlugin {
	
	List<String> messagesToInspect;

	public Broker installPlugin(Broker broker) throws Exception {
		return new IsolationBroker(broker, messagesToInspect);
	}

	public List<String> getMessagesToInspect() {
		return messagesToInspect;
	}

	public void setMessagesToInspect(List<String> messagesToInspect) {
		this.messagesToInspect = messagesToInspect;
	}
}
