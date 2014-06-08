package forklift.consumer;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.mockito.Mockito;
import org.springframework.stereotype.Component;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;

@Component
public class MockConnector implements ForkliftConnectorI {
    private ForkliftConnectorI mock;

    public MockConnector() throws ConnectorException, JMSException {
        Message msg = Mockito.mock(Message.class);

        MessageConsumer consumer = Mockito.mock(MessageConsumer.class);
        Mockito.when(consumer.receive()).thenReturn(msg);

        mock = Mockito.mock(ForkliftConnectorI.class);
        Mockito.when(mock.getQueue(Mockito.anyString())).thenReturn(consumer);
    }

    @Override
    public void start() throws ConnectorException {
        mock.start();
    }

    @Override
    public void stop() throws ConnectorException {
        mock.stop();
    }

    @Override
    public Connection getConnection()
      throws ConnectorException {
        return mock.getConnection();
    }

    @Override
    public MessageConsumer getQueue(String name)
      throws ConnectorException {
        return mock.getQueue(name);
    }

    @Override
    public MessageConsumer getTopic(String name)
      throws ConnectorException {
        return mock.getTopic(name);
    }

    @Override
    public ForkliftMessage jmsToForklift(Message m) {
        ForkliftMessage msg = new ForkliftMessage();
        msg.setMsg("TEST MSG");
        return msg;
    }
}
