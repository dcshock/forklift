package forklift.consumer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Topic;

public class Listener implements Runnable {
    private static AtomicInteger id = new AtomicInteger(1);
    private AtomicBoolean running = new AtomicBoolean(false);

    private Queue queue;
    private Topic topic;
    private Class<?> msgHandler;
    private ForkliftConnectorI connector;
    private MessageConsumer consumer;
    private String name;

    private List<Method> methods = new ArrayList<Method>();

    public Listener(Queue queue, Topic topic, Class<?> msgHandler,
                    ForkliftConnectorI connector) {
        super();
        this.queue = queue;
        this.topic = topic;
        this.msgHandler = msgHandler;
        this.connector = connector;
        this.name = "" + id.getAndIncrement();
    }

    public void listen() {
        // Look for all methods that need to be called when a
        // message is received.
        for (Method m : msgHandler.getDeclaredMethods())
            if (m.isAnnotationPresent(OnMessage.class))
                methods.add(m);

        try {
            if (topic != null)
                consumer = connector.getTopic("topic://" + topic.value());
            else if (queue != null)
                consumer = connector.getQueue("queue://" + queue.value());
            else
                throw new RuntimeException("No queue/topic specified");
        } catch (ConnectorException e) {
            e.printStackTrace();
        }

        final Thread t = new Thread(getName());
        t.run();
    }

    public String getName() {
        return name;
    }

    @Override
    public void run() {
        try {
            running.set(true);

            Message jmsMsg;
            while (running.get() && (jmsMsg = consumer.receive()) != null) {
                final ForkliftMessage msg = connector.jmsToForklift(jmsMsg);
                try {
                    final Object handler = msgHandler.newInstance();
                    for (Method m : methods) {
                        // Send the message to each handler.
                        m.invoke(handler, new Object[] {});
                    }

                    msg.getMsg();
                    jmsMsg.acknowledge();
                } catch (Exception e) {
                    // Avoid acking a msg that hasn't been processed successfully.
                }
            }
        } catch (JMSException e) {
            running.set(false);
            e.printStackTrace();
        }
    }
}
