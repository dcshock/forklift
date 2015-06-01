package forklift.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Callback;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.decorators.Config;
import forklift.decorators.Headers;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.producers.ForkliftProducerI;
import forklift.properties.PropertiesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    private final ClassLoader classLoader;
    private final ForkliftConnectorI connector;
    private final Map<Class, Map<Class<?>, List<Field>>> injectFields;
    private final Class<?> msgHandler;
    private final String name;
    private final List<Method> onMessage;
    private final List<Method> onValidate;
    private final Queue queue;
    private final Topic topic;
    private final ApplicationContext context;

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
        this(msgHandler, connector, classLoader, null);
    }

    @SuppressWarnings("unchecked")
    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader, ApplicationContext context) {
        this.classLoader = classLoader;
        this.connector = connector;
        this.msgHandler = msgHandler;
        this.context = context;
        this.topic = msgHandler.getAnnotation(Topic.class);
        this.queue = msgHandler.getAnnotation(Queue.class);

        if (this.queue != null && this.topic != null)
            throw new IllegalArgumentException("Msg Handler cannot consume a queue and topic");
        if (this.queue != null)
            this.name = queue.value() + ":" + id.getAndIncrement();
        else if (this.topic != null)
            this.name = topic.value() + ":" + id.getAndIncrement();
        else
            throw new IllegalArgumentException("Msg Handler must handle a queue or topic.");

        log = LoggerFactory.getLogger(this.name);

        // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
        // it'll just run in the current thread to prevent any message read ahead that would be performed.
        if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
            MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
            log.info("Creating thread pool of {}", multiThreaded.value());
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
        onValidate = new ArrayList<>();
        for (Method m : msgHandler.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        injectFields = new HashMap<>();
        injectFields.put(Config.class, new HashMap<>());
        injectFields.put(javax.inject.Inject.class, new HashMap<>());
        injectFields.put(forklift.decorators.Message.class, new HashMap<>());
        injectFields.put(forklift.decorators.Headers.class, new HashMap<>());
        injectFields.put(forklift.decorators.Properties.class, new HashMap<>());
        injectFields.put(forklift.decorators.Producer.class, new HashMap<>());
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
            log.debug("", e);
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
                        final MessageRunnable runner = new MessageRunnable(this, msg, classLoader, handler, onMessage, onValidate);
                        if (threadPool != null)
                            threadPool.execute(runner);
                        else
                            runner.run();
                    } catch (Exception e) {
                        // If this error occurs we had a massive problem with the conusmer class setup.
                        log.error("Consumer couldn't be used.", e);

                        // In this instance just stop the consumer. Someone needs to fix their shit!
                        running.set(false);
                    }
                }

                if (outOfMessages != null)
                    outOfMessages.handle(this);
            }

            // Shutdown the pool, but let actively executing work finish.
            if (threadPool != null) {
                log.info("Shutting down thread pool - active {}", threadPool.getActiveCount());
                threadPool.shutdown();
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

    /**
     * Inject the data from a forklift message into an instance of the msgHandler class.
     * @param msg containing data
     * @param instance an instance of the msgHandler class.
     */
    public void inject(ForkliftMessage msg, final Object instance) {
        // Inject the forklift msg
        injectFields.keySet().stream().forEach(decorator -> {
            final Map<Class<?>, List<Field>> fields = injectFields.get(decorator);

            fields.keySet().stream().forEach(clazz -> {
                fields.get(clazz).forEach(f -> {
                    try {
                        if (decorator == forklift.decorators.Message.class) {
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
                        } else if (decorator == javax.inject.Inject.class) {
                            if (clazz ==  ApplicationContext.class) {
                                f.set(instance, context);
                            } else {
                                f.set(instance, context.getBean(clazz));
                            }
                        } else if (decorator == Config.class) {
                            if (clazz == Properties.class) {
                                forklift.decorators.Config config = f.getAnnotation(forklift.decorators.Config.class);
                                f.set(instance, PropertiesManager.get(config.value()));
                            }
                        } else if (decorator == Headers.class) {
                            if (clazz == Map.class) {
                                f.set(instance, msg.getHeaders());
                            }
                        } else if (decorator == forklift.decorators.Properties.class) {
                            if (clazz == Map.class) {
                                f.set(instance, msg.getProperties());
                            }
                        } else if (decorator == forklift.decorators.Producer.class) {
                            if (clazz == ForkliftProducerI.class) {
                                forklift.decorators.Producer producer = f.getAnnotation(forklift.decorators.Producer.class);
                                if (producer.queue().length() > 0) {
                                    f.set(instance, connector.getQueueProducer(producer.queue()));
                                } else if (producer.topic().length() > 0) {
                                    f.set(instance, connector.getTopicProducer(producer.topic()));
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error injecting data into Msg Handler", e);
                        throw new RuntimeException("Error injecting data into Msg Handler");
                    }
                });
            });
        });
    }

    public Class<?> getMsgHandler() {
        return msgHandler;
    }

    public Queue getQueue() {
        return queue;
    }

    public Topic getTopic() {
        return topic;
    }

    public ForkliftConnectorI getConnector() {
        return connector;
    }
}
