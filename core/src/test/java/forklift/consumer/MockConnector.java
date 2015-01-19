package forklift.consumer;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.stereotype.Component;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;

@Component
public class MockConnector implements ForkliftConnectorI {
    List<Message> msgs = new ArrayList<Message>();

    private ForkliftConnectorI mock;

    public MockConnector() throws ConnectorException, JMSException {
        final MessageConsumer consumer = Mockito.mock(MessageConsumer.class);

        final Answer<Message> answer = new Answer<Message>() {
            @Override
            public Message answer(InvocationOnMock invocation) throws Throwable {
                if (msgs.size() == 0)
                    return null;

                return msgs.remove(0);
            }

        };

        Mockito.when(consumer.receive()).thenAnswer(answer);
        Mockito.when(consumer.receive(Mockito.anyLong())).thenAnswer(answer);

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

    public void addMsg() {
        msgs.add(Mockito.mock(Message.class));
    }
}
