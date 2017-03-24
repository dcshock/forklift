package forklift.consumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;

public interface ForkliftConsumerI {
	ForkliftMessage receive(long timeout) throws ConnectorException;
	void close() throws ConnectorException;
}
