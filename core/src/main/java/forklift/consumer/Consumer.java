package forklift.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Callback;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.decorators.Audit;
import forklift.decorators.Config;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Retry;
import forklift.decorators.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

public class Consumer {
    private Logger log;

    private static AtomicInteger id = new AtomicInteger(1);
    private static ObjectMapper mapper = new ObjectMapper();

    private final Boolean audit;
    private final ClassLoader classLoader;
    private final ForkliftConnectorI connector;
    private final Map<Class, Map<Class<?>, List<Field>>> injectFields;
    private final Class<?> msgHandler;
    private final String name;
    private final List<Method> onMessage;
    private final Retry retry;
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

    @SuppressWarnings("unchecked")
    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader) {
        this.audit = msgHandler.isAnnotationPresent(Audit.class);
        this.classLoader = classLoader;
        this.connector = connector;
        this.msgHandler = msgHandler;
        this.retry = msgHandler.getAnnotation(Retry.class);
        this.queue = msgHandler.getAnnotation(Queue.class);
        this.name = queue.value() + ":" + id.getAndIncrement();
        this.topic = msgHandler.getAnnotation(Topic.class);

        log = LoggerFactory.getLogger(this.name);

        // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
        // it'll just run in the current thread to prevent any message read ahead that would be performed.
        if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
            MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
            blockQueue = new ArrayBlockingQueue<Runnable>(multiThreaded.value() * 100 + 100);
            threadPool = new ThreadPoolExecutor(
                Math.min(2, multiThreaded.value()), multiThreaded.value(), 5L, TimeUnit.MINUTES, blockQueue);
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

            injectFields = new HashMap<>();
            injectFields.put(Config.class, new HashMap<>());
            injectFields.put(forklift.decorators.Message.class, new HashMap<>());
            for (Field f : msgHandler.getDeclaredFields()) {
                injectFields.keySet().forEach(type -> {
                    if (f.isAnnotationPresent(type)) {
                        f.setAccessible(true);

                    // Init the list
                        if (injectFields.get(type).get(f.getType()) == null)
                            injectFields.get(type).put(f.getType(), new ArrayList<>());
                        injectFields.get(type).get(f.getType()).add(f);
                    }
                });
            }
        }

    /**
     * Creates a JMS consumer and begins listening for messages.
     * If the JMS consumer dies, this method will attempt to
     * get a new JMS consumer.
     */
    public void listen() {
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

                        RunAsClassLoader.run(classLoader, () -> {
                            inject(msg, handler);
                        });

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
                        // TODO - Audit Errors here, and start acking..
                        // Avoid acking a msg that hasn't been processed successfully.
                    }
                }

                if (outOfMessages != null)
                    outOfMessages.handle(this);
            }
        } catch (JMSException e) {
            running.set(false);
            log.error("JMS Error in message loop: ", e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error in message loop shutdown:", e);
            }
        }
    }

    public void shutdown() {
        running.set(false);
    }

    public void setOutOfMessages(Callback<Consumer> outOfMessages) {
        this.outOfMessages = outOfMessages;
    }

    private void inject(ForkliftMessage msg, final Object instance) {
        final Map<Class<?>, List<Field>> fields = injectFields.get(forklift.decorators.Message.class);

        // Inject the forklift msg
        fields.keySet().stream().forEach(clazz -> {
            fields.get(clazz).forEach(f -> {
                try {
                    if (clazz ==  ForkliftMessage.class) {
                        f.set(instance, msg);
                    } else if (clazz == String.class) {
                        f.set(instance, msg.getMsg());
                    } else if (clazz == Map.class) {
                        // We assume that the map is <String, String>.
                        f.set(instance, KeyValueParser.parse(msg.getMsg()));
                    } else {
                        // Attempt to parse a json
                        f.set(instance, mapper.readValue(msg.getMsg(), clazz));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
