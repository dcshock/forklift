package forklift.consumer;

import forklift.Forklift;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.BlockingRejectedExecutionHandler;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.decorators.Config;
import forklift.decorators.Headers;
import forklift.decorators.MultiThreaded;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.properties.PropertiesManager;
import forklift.source.SourceI;
import forklift.source.SourceUtil;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.TopicSource;
import forklift.source.decorators.Queue;
import forklift.source.decorators.Topic;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

public class Consumer {
    static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                   .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private Logger log;
    private static AtomicInteger id = new AtomicInteger(1);

    private final ClassLoader classLoader;
    private final Forklift forklift;

    private final Class<?> msgHandler;
    private Constructor<?> constructor;
    private Annotation[][] constructorAnnotations;
    private String name;
    private SourceI source;
    private List<SourceI> roleSources = Collections.emptyList();
    private List<ConsumerService> services;
    private final ConsumerAnnotationDetector annotations;

    // If a queue can process multiple messages at a time we
    // use a thread pool to manage how much cpu load the queue can
    // take. These are reinstantiated anytime the consumer is asked
    // to listen for messages.
    private BlockingQueue<Runnable> blockQueue;
    private ThreadPoolExecutor threadPool;

    private java.util.function.Consumer<Consumer> outOfMessages;

    private AtomicBoolean running = new AtomicBoolean(false);

    public Consumer(Class<?> msgHandler, Forklift forklift) {
        this(msgHandler, forklift, null);
    }

    public Consumer(Class<?> msgHandler, Forklift forklift, ClassLoader classLoader) {
        this(msgHandler, forklift, classLoader, false);
    }

    public Consumer(Class<?> msgHandler, Forklift forklift, ClassLoader classLoader, Queue queue) {
        this(msgHandler, forklift, classLoader, true);
        if (queue == null)
            throw new IllegalArgumentException("Msg Handler must handle a queue.");

        this.source = new QueueSource(queue);
        this.name = queue.value() + ":" + id.getAndIncrement();

        log = LoggerFactory.getLogger(this.name);
    }

    public Consumer(Class<?> msgHandler, Forklift forklift, ClassLoader classLoader, Topic topic) {
        this(msgHandler, forklift, classLoader, true);
        if (topic == null)
            throw new IllegalArgumentException("Msg Handler must handle a topic.");

        this.source = new TopicSource(topic);
        this.name = topic.value() + ":" + id.getAndIncrement();

        log = LoggerFactory.getLogger(this.name);
    }

    public Consumer(Class<?> msgHandler, Forklift forklift, ClassLoader classLoader, SourceI source, List<SourceI> roleSources) {
        this(msgHandler, forklift, classLoader, true);
        this.source = source;
        this.roleSources = roleSources;

        this.name = source
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .apply(RoleInputSource.class, roleSource -> roleSource.getRole())
            .get() + ":" + id.getAndIncrement();
        log = LoggerFactory.getLogger(this.name);
    }

    @SuppressWarnings("unchecked")
    private Consumer(Class<?> msgHandler, Forklift forklift, ClassLoader classLoader, boolean preinit) {
        this.classLoader = classLoader;
        this.forklift = forklift;
        this.msgHandler = msgHandler;

        if (!preinit && source == null) {
            this.roleSources = SourceUtil.getSourcesAsList(msgHandler);

            if (this.roleSources.size() > 1)
                throw new IllegalArgumentException("One consumer instance cannot consume more than one source");
            if (this.roleSources.size() == 0)
                throw new IllegalArgumentException("A consumer must consume at least one source");

            this.source = roleSources.get(0);
            this.name = source
                .apply(QueueSource.class, queue -> queue.getName())
                .apply(TopicSource.class, topic -> topic.getName())
                .apply(GroupedTopicSource.class, topic -> topic.getName())
                .get() + ":" + id.getAndIncrement();
        }

        log = LoggerFactory.getLogger(Consumer.class);
        this.annotations =
            new ConsumerAnnotationDetector(msgHandler,
                forklift.getConnector().supportsOrder(), forklift.getConnector().supportsResponse());
        configureConstructorInjection();
    }

    /**
     * Creates a JMS consumer and begins listening for messages.
     * If the JMS consumer dies, this method will attempt to
     * get a new JMS consumer.
     */
    public void listen() {
        final ForkliftConsumerI consumer;
        try {
            consumer = forklift.getConnector().getConsumerForSource(source);

            // Init the thread pools if the msg handler is multi threaded. If the msg handler is single threaded
            // it'll just run in the current thread to prevent any message read ahead that would be performed.
            if (msgHandler.isAnnotationPresent(MultiThreaded.class)) {
                final MultiThreaded multiThreaded = msgHandler.getAnnotation(MultiThreaded.class);
                log.info("Creating thread pool of {}", multiThreaded.value());
                blockQueue = new ArrayBlockingQueue<>(multiThreaded.value() * 100 + 100);
                threadPool = new ThreadPoolExecutor(
                                                       multiThreaded.value(), multiThreaded.value(), 5L, TimeUnit.MINUTES, blockQueue);
                threadPool.setRejectedExecutionHandler(new BlockingRejectedExecutionHandler());
            } else {
                blockQueue = null;
                threadPool = null;
            }

            messageLoop(consumer);

            // Always cleanup the consumer.
            if (consumer != null)
                consumer.close();
        } catch (ConnectorException e) {
            log.error("Error getting consumer from connector", e);
        }
    }

    public String getName() {
        return name;
    }

    public void messageLoop(ForkliftConsumerI consumer) {
        Map<String, List<MessageRunnable>> orderQueue = annotations.getOrderQueue();
        Method orderMethod = annotations.getOrderMethod();
        List<Method> onMessage = annotations.getOnMessage();
        List<Method> onValidate = annotations.getOnValidate();
        List<Method> onResponse = annotations.getOnResponse();
        Map<ProcessStep, List<Method>> onProcessStep = annotations.getOnProcessStep();
        try {
            running.set(true);

            while (running.get()) {
                ForkliftMessage consumerMsg;
                while ((consumerMsg = consumer.receive(2500)) != null && running.get()) {
                    try {
                        final List<Closeable> closeMe = new ArrayList<>();
                        final Object handler = constructMessageHandlerInstance(consumerMsg, closeMe);

                        final ForkliftMessage msg = consumerMsg;

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
                    outOfMessages.accept(this);
            }

            // Shutdown the pool, but let actively executing work finish.
            if (threadPool != null) {
                log.info("Shutting down thread pool - active {}", threadPool.getActiveCount());
                threadPool.shutdown();
                blockQueue.clear();

                threadPool.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (ConnectorException e) {
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
        log.info("Consumer shutting down");
        running.set(false);
    }

    public void setOutOfMessages(java.util.function.Consumer<Consumer> outOfMessages) {
        this.outOfMessages = outOfMessages;
    }

    private final void configureConstructorInjection() {
        Constructor<?>[] constructors = msgHandler.getDeclaredConstructors();
        List<Constructor> injectableConstructors = Arrays.stream(constructors).filter(constructor -> constructor.isAnnotationPresent(Inject.class)).collect(Collectors.toList());
        if (injectableConstructors.size() > 0) {
            this.constructor = injectableConstructors.get(0);
            this.constructorAnnotations = this.constructor.getParameterAnnotations();
            if (injectableConstructors.size() > 1) {
                log.error("Multiple constructors annotated with Inject.  Using first injectable constructor found");
            }
        }
    }

    /**
     * Inject the data from a forklift message into an instance of the msgHandler class.
     *
     * @param msg      containing data
     * @param instance an instance of the msgHandler class.
     */
    public List<Closeable> inject(ForkliftMessage msg, final Object instance) {
        // Keep any closable resources around so the injection utilizer can cleanup.
        final List<Closeable> closeMe = new ArrayList<>();

        Map<Class, Map<Class<?>, List<Field>>> injectFields = annotations.getInjectFields();
        // Inject the forklift msg
        injectFields.keySet().stream().forEach(decorator -> {
            final Map<Class<?>, List<Field>> fields = injectFields.get(decorator);

            fields.keySet().stream().forEach(clazz -> {
                fields.get(clazz).forEach(field -> {
                    log.trace("Inject target> Field: ({})  Decorator: ({})", field, decorator);
                    try {
                        Object value = getInjectableValue(field.getAnnotation(decorator), field.getName(), clazz, msg);
                        if (value instanceof ForkliftProducerI) {
                            closeMe.add((ForkliftProducerI)value);
                        }
                        if (value != null) {
                            field.set(instance, value);
                        }
                    } catch (JsonMappingException | JsonParseException e) {
                        log.warn("Unable to parse json for injection.", e);
                    } catch (Exception e) {
                        log.error("Error injecting data into Msg Handler", e);
                        e.printStackTrace();
                        throw new RuntimeException("Error injecting data into Msg Handler");
                    }
                });
            });
        });

        return closeMe;
    }

    private Object constructMessageHandlerInstance(ForkliftMessage forkliftMessage, List<Closeable> closeables) throws IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        Object instance = null;
        if (this.constructor != null) {
            Object[] constructorParameters = buildConstructorParameters(forkliftMessage, closeables);
            instance = this.constructor.newInstance(constructorParameters);
        } else {
            instance = msgHandler.newInstance();
        }
        return instance;
    }

    private Object[] buildConstructorParameters(ForkliftMessage forkliftMessage, List<Closeable> closeables) throws IOException {
        Map<Class, Map<Class<?>, List<Field>>> injectFields = annotations.getInjectFields();
        Object[] parameters = new Object[constructorAnnotations.length];
        int index = 0;
        for (Annotation[] parameterAnnotations : constructorAnnotations) {
            Annotation injectable = null;
            for (Annotation parameterAnnotation : parameterAnnotations) {
                if (injectFields.containsKey(parameterAnnotation.annotationType())) {
                    injectable = parameterAnnotation;
                    break;
                }
            }
            Parameter p = constructor.getParameters()[index];
            Object value = getInjectableValue(injectable, null, p.getType(), forkliftMessage);
            parameters[index] = value;
            if (value != null && value instanceof ForkliftProducerI) {
                closeables.add((ForkliftProducerI)value);
            }

            index++;
        }
        return parameters;
    }

    private Object getInjectableValue(Annotation decorator, String mappedName, Class<?> mappedClass, ForkliftMessage msg) throws IOException {
        Object value = null;
        if (decorator == null || decorator.annotationType() == javax.inject.Inject.class) {
            if (this.services != null) {
                // Try to resolve the class from any available BeanResolvers.
                for (ConsumerService s : this.services) {
                    try {
                        final Object o = s.resolve(mappedClass, null);
                        if (o != null) {
                            value = o;
                            break;
                        }
                    } catch (Exception e) {
                        log.debug("", e);
                    }
                }
            }
        } else if (decorator.annotationType() == forklift.decorators.Message.class && msg.getMsg() != null) {
            if (mappedClass == ForkliftMessage.class) {
                value = msg;
            } else if (mappedClass == String.class) {
                value = msg.getMsg();
            } else if (mappedClass == Map.class && !msg.getMsg().startsWith("{")) {
                value = KeyValueParser.parse(msg.getMsg());
                // We assume that the map is <String, String>.
            } else {
                // Attempt to parse a json
                value = mapper.readValue(msg.getMsg(), mappedClass);
            }
        } else if (decorator.annotationType() == Config.class) {
            if (mappedClass == Properties.class) {
                String confName = ((Config)decorator).value();
                if (confName.equals("")) {
                    confName = mappedName;
                }
                final Properties config = PropertiesManager.get(confName);
                if (config == null) {
                    log.warn("Attempt to inject field failed because resource file {} was not found", ((Config)decorator).value());
                } else {
                    value = config;
                }
            } else {
                final Properties config = PropertiesManager.get(((Config)decorator).value());
                if (config == null) {
                    log.warn("Attempt to inject field failed because resource file {} was not found", ((Config)decorator).value());
                }
                String key = ((Config)decorator).field();
                if (key.equals("")) {
                    key = mappedName;
                }
                value = config.get(key);
            }
        } else if (decorator.annotationType() == Headers.class) {
            final Map<Header, Object> headers = msg.getHeaders();
            if (mappedClass == Map.class) {
                value = headers;
            } else {
                final Header key = ((Headers)decorator).value();
                if (headers == null) {
                    log.warn("Attempt to inject {} from headers, but headers are null", key);
                } else if (!key.getHeaderType().equals(mappedClass)) {
                    log.warn("Injecting field {} failed because it is not type {}", mappedName, key.getHeaderType());
                } else {
                    value = headers.get(key);
                }
            }
        } else if (decorator.annotationType() == forklift.decorators.Properties.class) {
            Map<String, String> properties = msg.getProperties();
            if (mappedClass == Map.class) {
                value = msg.getProperties();
            } else if (properties != null) {
                String key = ((forklift.decorators.Properties)decorator).value();
                if (key.equals("")) {
                    key = mappedName;
                }
                if (properties == null) {
                    log.warn("Attempt to inject field {} from properties, but properties is null", key);
                } else {
                    value = properties.get(key);
                }
            }
        } else if (decorator.annotationType() == forklift.decorators.Producer.class) {
            if (mappedClass == ForkliftProducerI.class) {
                forklift.decorators.Producer producer = (forklift.decorators.Producer)decorator;
                final ForkliftProducerI p;
                if (producer.queue().length() > 0)
                    p = forklift.getConnector().getQueueProducer(producer.queue());
                else if (producer.topic().length() > 0)
                    p = forklift.getConnector().getTopicProducer(producer.topic());
                else
                    p = null;
                value = p;
            }
        }
        return value;
    }

    public Class<?> getMsgHandler() {
        return msgHandler;
    }

    public Forklift getForklift() {
        return forklift;
    }

    /**
     * Creates an instance of the MessageHandler class utilized by this constructor.  Constructor and Field level injection is performed using both the
     * passed in msg and any Services {@link #addServices(ConsumerService...) added} to this consumer.
     *
     * @param msg the message used for injection
     */
    public Object getMsgHandlerInstance(ForkliftMessage msg) {
        Object instance = null;
        try {
            instance = this.constructMessageHandlerInstance(msg, new ArrayList<>());
            inject(msg, instance);
        } catch (JsonMappingException | JsonParseException e) {
            log.warn("Unable to parse json for injection.", e);
        } catch (Exception e) {
            log.error("Error injecting data into Msg Handler", e);
            e.printStackTrace();
            throw new RuntimeException("Error injecting data into Msg Handler Constructor");
        }
        return instance;
    }

    public SourceI getSource() {
        return source;
    }

    public List<SourceI> getRoleSources() {
        return roleSources;
    }

    public <SOURCE extends SourceI> Stream<SOURCE> getRoleSources(Class<SOURCE> sourceType) {
        return roleSources.stream()
            .filter(source -> sourceType.isInstance(source))
            .map(source -> {
                try {
                    return sourceType.cast(source);
                } catch (ClassCastException e) { // should be impossible
                    log.error("Impossible class cast exception; sound the alarms", e);
                }
                return null;
            });
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
