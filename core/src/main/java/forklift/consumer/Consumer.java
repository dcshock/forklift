package forklift.consumer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Callback;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.decorators.Config;
import forklift.decorators.Headers;
import forklift.decorators.MultiThreaded;
import forklift.decorators.On;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Ons;
import forklift.decorators.Order;
import forklift.decorators.Queue;
import forklift.decorators.Response;
import forklift.decorators.Topic;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.properties.PropertiesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;

public class Consumer {
    static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                   .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private Logger log;
    private static AtomicInteger id = new AtomicInteger(1);

    private final ClassLoader classLoader;
    private final ForkliftConnectorI connector;
    private final Map<Class, Map<Class<?>, List<Field>>> injectFields;
    private final Class<?> msgHandler;
    private final List<Method> onMessage;
    private final List<Method> onValidate;
    private final List<Method> onResponse;
    private final Map<String, List<MessageRunnable>> orderQueue;
    private final Map<ProcessStep, List<Method>> onProcessStep;
    private String name;
    private Queue queue;
    private Topic topic;
    private List<ConsumerService> services;
    private Method orderMethod;

    // If a queue can process multiple messages at a time we
    // use a thread pool to manage how much cpu load the queue can
    // take. These are reinstantiated anytime the consumer is asked
    // to listen for messages.
    private BlockingQueue<Runnable> blockQueue;
    private ThreadPoolExecutor threadPool;

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

        // Look for all methods that need to be called when a
        // message is received.
        onMessage = new ArrayList<>();
        onValidate = new ArrayList<>();
        onResponse = new ArrayList<>();
        onProcessStep = new HashMap<>();
        Arrays.stream(ProcessStep.values()).forEach(step -> onProcessStep.put(step, new ArrayList<>()));
        for (Method m : msgHandler.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(Response.class))
                onResponse.add(m);
            else if (m.isAnnotationPresent(Order.class))
                orderMethod = m;
            else if (m.isAnnotationPresent(On.class) || m.isAnnotationPresent(Ons.class))
                Arrays.stream(m.getAnnotationsByType(On.class)).map(on -> on.value()).distinct().forEach(x -> onProcessStep.get(x).add(m));
        }

        if (orderMethod != null)
            orderQueue = new HashMap<>();
        else
            orderQueue = null;

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
        final ForkliftConsumerI consumer;
        try {
            if (topic != null)
                consumer = connector.getTopic(topic.value());
            else if (queue != null)
                consumer = connector.getQueue(queue.value());
            else
                throw new RuntimeException("No queue/topic specified");

            // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
            // it'll just run in the current thread to prevent any message read ahead that would be performed.
            if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
                final MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
                log.info("Creating thread pool of {}", multiThreaded.value());
                blockQueue = new ArrayBlockingQueue<>(multiThreaded.value() * 100 + 100);
                threadPool = new ThreadPoolExecutor(
                    multiThreaded.value(), multiThreaded.value(), 5L, TimeUnit.MINUTES, blockQueue);
                threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            } else {
                blockQueue = null;
                threadPool = null;
            }

            messageLoop(consumer);

            // Always cleanup the consumer.
            if (consumer != null)
                consumer.close();
        } catch (ConnectorException | JMSException e) {
            log.debug("", e);
        }
    }

    public String getName() {
        return name;
    }

    public void messageLoop(ForkliftConsumerI consumer) {
        try {
            running.set(true);

            while (running.get()) {
                Message jmsMsg;
                while ((jmsMsg = consumer.receive(2500)) != null && running.get()) {
                    final ForkliftMessage msg = connector.jmsToForklift(jmsMsg);
                    try {
                        final Object handler = msgHandler.newInstance();

                        final List<Closeable> closeMe = new ArrayList<>();
                        RunAsClassLoader.run(classLoader, () -> {
                            closeMe.addAll(inject(msg, handler));
                        });

                        // Create the runner that will ultimately run the handler.
                        final MessageRunnable runner = new MessageRunnable(this, msg, classLoader, handler, onMessage, onValidate, onResponse, onProcessStep, closeMe);

                        // If the message is ordered we need to store messages that cannot currently be processed, and retry them periodically.
                        if (orderQueue != null) {
                            final String id = (String)orderMethod.invoke(handler);

                            // Reuse the close functionality to hook the process to trigger the next message execution.
                            closeMe.add(new Closeable() {
                                @Override
                                public void close() throws IOException {
                                    synchronized (orderQueue) {
                                        final List<MessageRunnable> msgs = orderQueue.get(id);
                                        msgs.remove(runner);

                                        final Optional<MessageRunnable> optRunner = msgs.stream().findFirst();

                                        if (optRunner.isPresent()) {
                                            // Execute the message.
                                            if (threadPool != null)
                                                threadPool.execute(optRunner.get());
                                            else
                                                optRunner.get().run();
                                        } else {
                                            orderQueue.remove(id);
                                        }
                                    }
                                }
                            });

                            synchronized (orderQueue) {
                                // If the message is not the first with a given identifier we'll assume that
                                // another message is currently being processed and we'll be called later.
                                if (orderQueue.containsKey(id)) {
                                    orderQueue.get(id).add(runner);

                                    // Let the next message get processed since this one needs to wait.
                                    continue;
                                }

                                final List<MessageRunnable> list = new ArrayList<>();
                                list.add(runner);
                                orderQueue.put(id, list);
                            }
                        }

                        // Execute the message.
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
                threadPool.awaitTermination(60, TimeUnit.SECONDS);
                blockQueue.clear();
            }
        } catch (JMSException e) {
            running.set(false);
            log.error("JMS Error in message loop: ", e);
        } catch (InterruptedException ignored) {
            // thrown by threadpool.awaitterm
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
    public List<Closeable> inject(ForkliftMessage msg, final Object instance) {
        // Keep any closable resources around so the injection utilizer can cleanup.
        final List<Closeable> closeMe = new ArrayList<>();

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
                            } else if (clazz == Map.class && !msg.getMsg().startsWith("{")) {
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

                                final ForkliftProducerI p;
                                if (producer.queue().length() > 0)
                                    p = connector.getQueueProducer(producer.queue());
                                else if (producer.topic().length() > 0)
                                    p = connector.getTopicProducer(producer.topic());
                                else
                                    p = null;

                                if (p != null)
                                    closeMe.add(p);
                                f.set(instance, p);
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

        return closeMe;
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

    public void addServices(ConsumerService... services) {
        if (this.services == null)
            this.services = new ArrayList<>();

        for (ConsumerService s : services)
            this.services.add(s);
    }

    public void setServices(List<ConsumerService> services) {
        this.services = services;
    }
}
