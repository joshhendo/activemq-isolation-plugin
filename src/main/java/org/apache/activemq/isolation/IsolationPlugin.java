package org.apache.activemq.isolation;

import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.isolation.lock.LockProvider;

public class IsolationPlugin implements BrokerPlugin {

	LockProvider lockProvider;
	List<String> messagesToInspect;

	public Broker installPlugin(Broker broker) throws Exception {
		this.lockProvider = new LockProvider();
		return new IsolationBroker(broker, lockProvider, messagesToInspect);
	}

	public List<String> getMessagesToInspect() {
		return messagesToInspect;
	}

	public void setMessagesToInspect(List<String> messagesToInspect) {
		this.messagesToInspect = messagesToInspect;
	}
}
