package org.apache.activemq.isolation;

import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.isolation.lock.LockProvider;

public class IsolationPlugin implements BrokerPlugin {

	LockProvider lockProvider;
	String definitionFile;

	public Broker installPlugin(Broker broker) throws Exception {
		this.lockProvider = new LockProvider();
		return new IsolationBroker(broker, lockProvider, definitionFile);
	}

	public String getDefinitionFile() {
		return definitionFile;
	}

	public void setDefinitionFileString (String definitionFile) {
		this.definitionFile = definitionFile;
	}
}
