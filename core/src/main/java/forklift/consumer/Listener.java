package forklift.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
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

        new Thread(this).run();
    }

    public String getName() {
        return name;
    }

    @Override
    public void run() {
        try {
            running.set(true);
            Message m = consumer.receive();
            ForkliftMessage msg = connector.jmsToForklift(m);
            msg.getMsg();
            m.acknowledge();
        } catch (JMSException e) {
            running.set(false);
            e.printStackTrace();
        }
    }
}
