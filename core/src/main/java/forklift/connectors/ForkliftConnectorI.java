package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;

public interface ForkliftConnectorI {
    void start() throws ConnectorException;
    void stop() throws ConnectorException;
    ForkliftConsumerI getQueue(String name) throws ConnectorException;
    ForkliftConsumerI getTopic(String name) throws ConnectorException;
    ForkliftProducerI getQueueProducer(String name);
    ForkliftProducerI getTopicProducer(String name);

    default boolean supportsOrder() {
        return false;
    }

    default boolean supportsResponse() {
        return false;
    }

    default boolean supportsQueue() {
        return false;
    }

    default boolean supportsTopic() {
        return false;
    }
}
