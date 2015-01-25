package forklift.consumer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.concurrent.Callback;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.Audit;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Topic;

public class Consumer {
    private Logger log;

    private static AtomicInteger id = new AtomicInteger(1);

    private final Boolean audit;
    private final ClassLoader classLoader;
    private final ForkliftConnectorI connector;
    private final List<Field> forkliftMsgFields;
    private final Class<?> msgHandler;
    private final String name;
    private final List<Method> onMessage;
    private final Queue queue;
    private final Topic topic;
    
    // If a queue can process multiple messages at a time we
    // use a thread pool to manage how much cpu load the queue can
    // take.
    private final BlockingQueue<Runnable> blockQueue;
    private final ThreadPoolExecutor threadPool;

    private Callback<Consumer> outOfMessages;

    private AtomicBoolean running = new AtomicBoolean(false);
    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector) {
    	this(msgHandler, connector, null);
    }
    
    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader) {
    	this.audit = msgHandler.isAnnotationPresent(Audit.class);
    	this.classLoader = classLoader;
    	this.connector = connector;
    	this.msgHandler = msgHandler;
    	this.queue = msgHandler.getAnnotation(Queue.class);
    	this.name = queue.value() + ":" + id.getAndIncrement();
        this.topic = msgHandler.getAnnotation(Topic.class);

        log = LoggerFactory.getLogger(this.name);

        // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
        // it'll just run in the current thread to prevent any message read ahead that would be performed.
        if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
            MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
            blockQueue = new ArrayBlockingQueue<Runnable>(multiThreaded.value() * 100 + 100);
            threadPool = new ThreadPoolExecutor(multiThreaded.value(), multiThreaded.value(), 5L, TimeUnit.MINUTES, blockQueue);
        } else {
            blockQueue = null;
            threadPool = null;
        }

        // Look for all methods that need to be called when a
        // message is received.
        onMessage = new ArrayList<>();
        for (Method m : msgHandler.getDeclaredMethods())
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);

        forkliftMsgFields = new ArrayList<>();
        for (Field f : msgHandler.getDeclaredFields()) {
            if (f.isAnnotationPresent(forklift.decorators.Message.class)) {
                f.setAccessible(true);
                if (f.getType() == ForkliftMessage.class)
                    forkliftMsgFields.add(f);
                else
                    log.warn("Unknown @Message field type, ignoring injection of messages");
            }
        }
    }

    /**
     * Creates a JMS consumer and begins listening for messages.
     * If the JMS consumer dies, this method will attempt to
     * get a new JMS consumer.
     */
    public void listen() {
        // Restart the message loop if the connection is severed.
        while (true) {
        	final MessageConsumer consumer;
            try {
                if (topic != null)
                    consumer = connector.getTopic(topic.value());
                else if (queue != null)
                    consumer = connector.getQueue(queue.value());
                else
                    throw new RuntimeException("No queue/topic specified");

                messageLoop(consumer);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }

            // We're either going to try again, or call it quits.
            if (running.get())
            	// TODO - We need to implement some wait logic here to avoid entering a buzz loop
            	// trying to get a consumer from a dead connector. 
                log.info("Reconnecting");
            else
                break;
        };
    }

    public String getName() {
        return name;
    }

    public void messageLoop(MessageConsumer consumer) {
        try {
            running.set(true);

            while (running.get()) {
                Message jmsMsg;
                while ((jmsMsg = consumer.receive(2500)) != null) {
                    final ForkliftMessage msg = connector.jmsToForklift(jmsMsg);
                    try {
                        final Object handler = msgHandler.newInstance();

                        // Inject the forklift msg
                        for (Field f : forkliftMsgFields)
                            f.set(handler, msg);

                        // Handle the message.
                        final MessageRunnable runner = new MessageRunnable(classLoader, handler, onMessage);
                        if (threadPool != null) {
                            threadPool.execute(runner);
                        } else {
                            runner.run();
                        }

                        msg.getMsg();
                        jmsMsg.acknowledge();
                    } catch (Exception e) {
                        // Avoid acking a msg that hasn't been processed successfully.
                    }
                }

                if (outOfMessages != null)
                    outOfMessages.handle(this);
            }
        } catch (JMSException e) {
            running.set(false);
            e.printStackTrace();
        }
    }

    public void shutdown() {
        running.set(false);
    }

    public void setOutOfMessages(Callback<Consumer> outOfMessages) {
        this.outOfMessages = outOfMessages;
    }
}
