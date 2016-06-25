package org.apache.activemq.isolation;

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.isolation.lock.LockProvider;
import org.apache.activemq.isolation.lock.RedisLockProvider;

public class IsolationPlugin implements BrokerPlugin {

	LockProvider lockProvider;

	// From settings
	String definitionFile;
	String lockProviderType = null;

	private String[] allowableLockProviderTypes = new String[]{"memory", "redis"};

	public Broker installPlugin(Broker broker) throws Exception {
		System.out.println("Got a definition file of " + definitionFile);

		if (getLockProviderType() == null) {
			System.out.println("No lock provider specified. Default to 'memory'");
			setLockProviderType("memory");
		}

		if (!Arrays.asList(allowableLockProviderTypes).contains(getLockProviderType())) {
            System.out.println("Not recognized lock provider. Default to 'memory'");
            setLockProviderType("memory");
        }

        if (getLockProviderType() == "redis") {
            System.out.println("Using 'redis' lock provider.");
            this.lockProvider = new RedisLockProvider();
        } else {
            System.out.println("Using 'memory' lock provider. *Shouldn't be used for production.*");
            this.lockProvider = new LockProvider();
        }

		return new IsolationBroker(broker, lockProvider, definitionFile);
	}

	public String getDefinitionFile() {
		return definitionFile;
	}

	public void setDefinitionFile(String definitionFile) {
		this.definitionFile = definitionFile;
	}

	public String getLockProviderType() {
		return lockProviderType;
	}

	public void setLockProviderType(String lockProviderType) {
		if (lockProviderType != null){
			this.lockProviderType = lockProviderType.toLowerCase();
		}
	}
}
