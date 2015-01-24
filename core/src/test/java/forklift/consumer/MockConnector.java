package forklift.consumer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;

//@Component
public class MockConnector implements ForkliftConnectorI {
	private Map<String, MessageConsumer> queues = new HashMap<>();
	private Map<String, Queue<Message>> msgs = new HashMap<>();

	private ForkliftConnectorI mock = Mockito.mock(ForkliftConnectorI.class);

    public MockConnector() throws ConnectorException, JMSException {
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
    	if (queues.containsKey(name))
    		return queues.get(name);

    	final Queue<Message> queue = buildQueue(name);
    	
    	final Answer<Message> answerNoWait = new Answer<Message>() {
    		@Override
    		public Message answer(InvocationOnMock invocation) throws Throwable {
    			synchronized (queue) {
    				if (queue.isEmpty()) 
    					return null;
    				
    				return queue.peek();
    			}
    		}
    	};
    	
    	final Answer<Message> answerWait = new Answer<Message>() {
    		@Override
    		public Message answer(InvocationOnMock invocation) throws Throwable {
    			synchronized (queue) {
    				if (queue.isEmpty()) {
    					queue.wait(1);
    					
    					if (queue.isEmpty())
    						return null;
    				}
    				return queue.peek();
    			}
    		}
    	};
    	
    	final MessageConsumer consumer = Mockito.mock(MessageConsumer.class);
        try {
			Mockito.when(consumer.receive()).thenAnswer(answerNoWait);
			Mockito.when(consumer.receive(Mockito.anyLong())).thenAnswer(answerWait);
		} catch (JMSException ignored) {
		}

        return consumer;
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

    public void addMsg(String name) {
    	final Queue<Message> queue = buildQueue(name);
    	
    	final Answer<Object> ack = new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				synchronized (queue) {
					return queue.poll();
				}
			}
    	};
    	
    	final Message m = Mockito.mock(Message.class);
    	try {
			Mockito.doAnswer(ack).when(m).acknowledge();
		} catch (JMSException ignored) {
		}
    	
    	synchronized (queue) {
    		queue.add(m);
    	}
    }
    
    private Queue<Message> buildQueue(String name) {
    	synchronized (msgs) {
    		if (msgs.containsKey(name))
    			return msgs.get(name);
    		
    		final Queue<Message> queue = new LinkedList<>();
    		msgs.put(name, queue);
    		return queue;
    	}
    }

	@Override
	public MessageProducer getProducer(String name) {
		// TODO Auto-generated method stub
		return null;
	}
}
