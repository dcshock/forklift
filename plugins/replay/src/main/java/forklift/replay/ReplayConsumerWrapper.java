package forklift.replay;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ForkliftConsumerI;

public class ReplayConsumerWrapper implements ForkliftConsumerI {
    private ForkliftConsumerI consumer;
    public ReplayConsumerWrapper(ForkliftConsumerI consumer) {
        this.consumer = consumer;
    }

    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        ForkliftMessage input = consumer.receive(timeout);

        return input;
    }

    @Override
    public void close() throws ConnectorException {
        consumer.close();
    }
}
