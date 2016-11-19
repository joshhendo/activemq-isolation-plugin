package org.apache.activemq.isolation.virtual;

import org.apache.activemq.broker.Broker;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RetryQueue {
    private static ConcurrentLinkedQueue<RetryMonitorEntry> queue = new ConcurrentLinkedQueue<RetryMonitorEntry>();

    public static void Monitor(Broker entryBroker) throws Exception {
        while (true) {
            // Extract batch to re-process
            ArrayList<RetryMonitorEntry> batch = new ArrayList<RetryMonitorEntry>();
            RetryMonitorEntry entry = queue.poll();
            while (entry != null) {
                batch.add(entry);
                entry = queue.poll();
            }

            // Iterate over the batch and re-process
            for (RetryMonitorEntry currentEntry : batch) {
                entryBroker.send(currentEntry.getProducerExchange(), currentEntry.getMessage());
            }

            // Sleep for a short while then re-process next batch
            Thread.sleep(50);
        }
    }

    public static void addToQueue(RetryMonitorEntry entry) {
        queue.add(entry);
    }
}
