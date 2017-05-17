package forklift.consumer.wrapper;

import forklift.consumer.ForkliftConsumerI;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;

/**
 * A consumer that maps the input from a given consumer according to the
 * serialized format for {@link RoleInputMessage role messages}.
 * <br>
 * This mapping function for role input is a crucial piece that allows
 * {@link forklift.source.sources.RoleInputSource RoleInputSource}s to be
 * backed by any arbitrary source.
 */
public class RoleInputConsumerWrapper implements ForkliftConsumerI {
    private ForkliftConsumerI consumer;

    /**
     * Creates a consumer that delegates message retrieval to the
     * given consumer and maps whatever the consumer outputs based on the
     * assumption that all of the messages returned from the given consumer
     * were serialized as {@link RoleInputMessage}s.
     *
     * @param consumer the consumer to get unmapped messages from
     */
    public RoleInputConsumerWrapper(ForkliftConsumerI consumer) {
        this.consumer = consumer;
    }

    /**
     * Delegates to the underlying consumer, and tries to extract a role message
     * from any received messages.
     *
     * @param timeout the timeout of the underlying consumer
     * @return the
     * @throws ConnectorException if a connector error occurs in the underlying consumer
     */
    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        final ForkliftMessage initialMessage = consumer.receive(timeout);
        if (initialMessage == null)
            return null;

        final RoleInputMessage roleInputMessage = RoleInputMessage.fromString(initialMessage.getMsg());
        return roleInputMessage.toForkliftMessage(initialMessage);
    }

    /**
     * Closees the underlying consumer.
     *
     * @throws ConnectorException if a connector error occurs in the underlying consumer
     */
    @Override
    public void close() throws ConnectorException {
        consumer.close();
    }
}
