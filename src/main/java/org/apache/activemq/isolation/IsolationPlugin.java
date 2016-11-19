package org.apache.activemq.isolation;

import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.isolation.lock.LockProvider;

public class IsolationPlugin implements BrokerPlugin {

	LockProvider lockProvider;
	String definitionFile;
	String errorHandling;

	public Broker installPlugin(Broker broker) throws Exception {
		System.out.println("Got a definition file of " + definitionFile);

		this.lockProvider = new LockProvider();
		return new IsolationBroker(broker, lockProvider, definitionFile, errorHandling);
	}

	// Config values getters and setters
	public String getDefinitionFile() {
		return definitionFile;
	}

	public void setDefinitionFile(String definitionFile) {
		this.definitionFile = definitionFile;
	}

	public String getErrorHandling() {
		return errorHandling;
	}

	public void setErrorHandling(String errorHandling) {
		this.errorHandling = errorHandling;
	}
}
