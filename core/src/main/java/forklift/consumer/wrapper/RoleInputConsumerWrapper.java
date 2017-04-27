package forklift.consumer.wrapper;

import forklift.consumer.ForkliftConsumerI;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;

public class RoleInputConsumerWrapper implements ForkliftConsumerI {
    private ForkliftConsumerI consumer;
    public RoleInputConsumerWrapper(ForkliftConsumerI consumer) {
        this.consumer = consumer;
    }

    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        ForkliftMessage initialMessage = consumer.receive(timeout);
        RoleInputMessage roleInputMessage = RoleInputMessage.fromString(initialMessage.getMsg());

        return roleInputMessage.toForkliftMessage(initialMessage);
    }

    @Override
    public void close() throws ConnectorException {
        consumer.close();
    }
}
