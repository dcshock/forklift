package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;
import forklift.source.LogicalSourceContext;
import forklift.source.SourceI;

/**
 * An entity that manages consuming and producing to a particular message bus.
 */
public interface ForkliftConnectorI extends LogicalSourceContext {
    /**
     * Starts the given connector. This method must be called for calls to
     * connector methods to be expected to behave properly.
     *
     * @throws ConnectorException if there was a problem initializing the state
     *                            of this connector or making a connection
     */
    void start() throws ConnectorException;

    /**
     * Stops the given connector, invalidating it's current state.
     *
     * @throws ConnectorException if there was a problem un-initializing the state
     *                            of this connector
     */
    void stop() throws ConnectorException;

    /**
     * Retrieves the {@link ForkliftConsumerI consumer} instance that reads from
     * the given source.
     *
     * @param source the source to read from
     * @return a consumer that reads from the given source
     * @throws ConnectorException if an error occurred interacting with the connector
     * @throws RuntimeException   if reading from the given source is not supported
     */
    ForkliftConsumerI getConsumerForSource(SourceI source) throws ConnectorException;

    /**
     * Gives a {@link ForkliftProducerI producer} instance that writes
     * to the given queue.
     *
     * @param name the name of the queue to write to
     * @return a producer that writes to the given queue
     */
    ForkliftProducerI getQueueProducer(String name);

    /**
     * Gives a {@link ForkliftProducerI producer} instance that writes
     * to the given topic.
     *
     * @param name the name of the topic to write to
     * @return a producer that writes to the given topic
     */
    ForkliftProducerI getTopicProducer(String name);

    /**
     * Gives the {@link ForkliftSerializer serializer} to use for explicitly
     * pre-serializing messages destined to sources on this connector, but which
     * need to be sent by an external application.
     *
     * @return the serializer to use for serializing messages to this connector,
     * or null if there is no explicit serialization method for this connector
     */
    default ForkliftSerializer getDefaultSerializer() {
        return null;
    }

    /**
     * Does this connector support the {@link forklift.decorators.Order} annotation
     * on consumers?
     *
     * @return whether this connector supports {@code Order}
     */
    default boolean supportsOrder() {
        return false;
    }

    /**
     * Does this connector support the {@link forklift.decorators.Response} annotation
     * on consumers?
     *
     * @return whether this connector supports {@code Response}
     */
    default boolean supportsResponse() {
        return false;
    }
}
