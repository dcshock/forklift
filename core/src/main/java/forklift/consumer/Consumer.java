package forklift.consumer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
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
import forklift.decorators.On;
import forklift.decorators.Ons;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.properties.PropertiesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<Method> onMessage;
    private final List<Method> onValidate;
    private final Map<ProcessStep, List<Method>> onProcessStep;
    private String name;
    private Queue queue;
    private Topic topic;
    private List<ConsumerService> services;

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
        this(msgHandler, connector, classLoader, false);
    }

    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader, Queue q) {
        this(msgHandler, connector, classLoader, true);
        this.queue = q;

        if (this.queue == null)
            throw new IllegalArgumentException("Msg Handler must handle a queue.");

        this.name = queue.value() + ":" + id.getAndIncrement();
        log = LoggerFactory.getLogger(this.name);
    }

    public Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader, Topic t) {
        this(msgHandler, connector, classLoader, true);
        this.topic = t;

        if (this.topic == null)
            throw new IllegalArgumentException("Msg Handler must handle a topic.");

        this.name = topic.value() + ":" + id.getAndIncrement();
        log = LoggerFactory.getLogger(this.name);
    }

    @SuppressWarnings("unchecked")
    private Consumer(Class<?> msgHandler, ForkliftConnectorI connector, ClassLoader classLoader, boolean preinit) {
        this.classLoader = classLoader;
        this.connector = connector;
        this.msgHandler = msgHandler;

        if (!preinit && queue == null && topic == null) {
            this.queue = msgHandler.getAnnotation(Queue.class);
            this.topic = msgHandler.getAnnotation(Topic.class);

            if (this.queue != null && this.topic != null)
                throw new IllegalArgumentException("Msg Handler cannot consume a queue and topic");

            if (this.queue != null)
                this.name = queue.value() + ":" + id.getAndIncrement();
            else if (this.topic != null)
                this.name = topic.value() + ":" + id.getAndIncrement();
            else
                throw new IllegalArgumentException("Msg Handler must handle a queue or topic.");

        }

        log = LoggerFactory.getLogger(Consumer.class);

        // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
        // it'll just run in the current thread to prevent any message read ahead that would be performed.
        if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
            MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
            log.info("Creating thread pool of {}", multiThreaded.value());
            blockQueue = new ArrayBlockingQueue<Runnable>(multiThreaded.value() * 100 + 100);
            threadPool = new ThreadPoolExecutor(
                Math.min(2, multiThreaded.value()), multiThreaded.value(), 5L, TimeUnit.MINUTES, blockQueue);
            threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        } else {
            blockQueue = null;
            threadPool = null;
        }

        // Look for all methods that need to be called when a
        // message is received.
        onMessage = new ArrayList<>();
        onValidate = new ArrayList<>();
        onProcessStep = new HashMap<>();
        Arrays.stream(ProcessStep.values()).forEach(step -> onProcessStep.put(step, new ArrayList<>()));
        for (Method m : msgHandler.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(On.class) || m.isAnnotationPresent(Ons.class))
                Arrays.stream(m.getAnnotationsByType(On.class)).map(on -> on.value()).distinct().forEach(x -> onProcessStep.get(x).add(m));
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
                        final MessageRunnable runner = new MessageRunnable(this, msg, classLoader, handler, onMessage, onValidate, onProcessStep);
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
                    log.trace("Inject target> Field: ({})  Decorator: ({})", f, decorator);
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
                        } else if (decorator == javax.inject.Inject.class && this.services != null) {
                            // Try to resolve the class from any available BeanResolvers.
                            for (ConsumerService s : this.services) {
                                try {
                                    final Object o = s.resolve(clazz, null);
                                    if (o != null) {
                                        f.set(instance, o);
                                        break;
                                    }
                                } catch (Exception e) {
                                    log.debug("", e);
                                }
                            }
                        } else if (decorator == Config.class) {
                            final forklift.decorators.Config annotation = f.getAnnotation(forklift.decorators.Config.class);
                            if (clazz == Properties.class) {
                                String confName = annotation.value();
                                if (confName.equals("")) {
                                    confName = f.getName();
                                }
                                final Properties config = PropertiesManager.get(confName);
                                if (config == null) {
                                    log.warn("Attempt to inject field failed because resource file {} was not found", annotation.value());
                                    return;
                                }
                                f.set(instance, config);
                            } else {
                                final Properties config = PropertiesManager.get(annotation.value());
                                if (config == null) {
                                    log.warn("Attempt to inject field failed because resource file {} was not found", annotation.value());
                                    return;
                                }
                                String key = annotation.field();
                                if (key.equals("")) {
                                    key = f.getName();
                                }
                                Object value = config.get(key);
                                if (value != null) {
                                    f.set(instance, value);
                                }
                            }
                        } else if (decorator == Headers.class) {
                            final Headers annotation = f.getAnnotation(Headers.class);
                            final Map<Header, Object> headers = msg.getHeaders();
                            if (clazz == Map.class) {
                                f.set(instance, headers);
                            } else {
                                final Header key = annotation.value();
                                if (headers == null) {
                                    log.warn("Attempt to inject {} from headers, but headers are null", key);
                                } else if (!key.getHeaderType().equals(f.getType())) {
                                    log.warn("Injecting field {} failed because it is not type {}", f.getName(), key.getHeaderType());
                                } else {
                                    final Object value = headers.get(key);
                                    if (value != null) {
                                        f.set(instance, value);
                                    }
                                }
                            }
                        } else if (decorator == forklift.decorators.Properties.class) {
                            forklift.decorators.Properties annotation = f.getAnnotation(forklift.decorators.Properties.class);
                            Map<String, Object> properties = msg.getProperties();
                            if (clazz == Map.class) {
                                f.set(instance, msg.getProperties());
                            } else if (properties != null) {
                                String key = annotation.value();
                                if (key.equals("")) {
                                    key = f.getName();
                                }
                                if (properties == null) {
                                    log.warn("Attempt to inject field {} from properties, but properties is null", key);
                                    return;
                                }
                                final Object value = properties.get(key);
                                if (value != null) {
                                    f.set(instance, value);
                                }
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
                    } catch (JsonMappingException | JsonParseException e) {
                        log.warn("Unable to parse json for injection.", e);
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

    public void setServices(List<ConsumerService> services) {
        this.services = services;
    }
}
