//package forklift.consumer;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import forklift.classloader.RunAsClassLoader;
//import forklift.connectors.ForkliftConnectorI;
//import forklift.connectors.ForkliftMessage;
//import forklift.consumer.parser.KeyValueParser;
//import forklift.decorators.Config;
//import forklift.decorators.Headers;
//import forklift.message.Header;
//import forklift.producers.ForkliftProducerI;
//import forklift.properties.PropertiesManager;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Closeable;
//import java.io.IOException;
//import java.lang.annotation.Annotation;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Field;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Parameter;
//import java.lang.reflect.Type;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import javax.inject.Inject;
//
//public class MessageHandlerProvider {
//
//    private static final Logger log = LoggerFactory.getLogger(MessageHandlerProvider.class);
//    static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
//                                                   .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//    private final Map<Class, Map<Class<?>, List<Field>>> injectFields = new HashMap<>();
//    private Constructor<?> constructor;
//    private final Class<?> messageHandler;
//    private final List<ConsumerService> services;
//    private final ForkliftConnectorI connector;
//    private final ClassLoader classLoader;
//
//    public MessageHandlerProvider(Class<?> messageHandler, ClassLoader classLoader, List<ConsumerService> services, ForkliftConnectorI connector) {
//        this.messageHandler = messageHandler;
//        this.services = services;
//        this.connector = connector;
//        this.classLoader = classLoader;
//        configureConstructorInjection();
//        configureFieldInjection();
//    }
//
//    private final void configureConstructorInjection() {
//        Constructor<?>[] constructors = this.messageHandler.getDeclaredConstructors();
//        for (Constructor<?> constructor : constructors) {
//            if (constructor.isAnnotationPresent(Inject.class)) {
//                this.constructor = constructor;
//                break;
//            }
//        }
//        if (this.constructor != null) {
//            this.constructor.getParameterAnnotations();
//            for (Parameter parameter : this.constructor.getParameters()) {
//
//            }
//        }
//        Annotation[] annotations = this.messageHandler.getAnnotationsByType(Inject.class);
//    }
//
//    private final void configureFieldInjection() {
//        injectFields.put(Config.class, new HashMap<>());
//        injectFields.put(javax.inject.Inject.class, new HashMap<>());
//        injectFields.put(forklift.decorators.Message.class, new HashMap<>());
//        injectFields.put(forklift.decorators.Headers.class, new HashMap<>());
//        injectFields.put(forklift.decorators.Properties.class, new HashMap<>());
//        injectFields.put(forklift.decorators.Producer.class, new HashMap<>());
//        for (Field f : messageHandler.getDeclaredFields()) {
//            injectFields.keySet().forEach(type -> {
//                if (f.isAnnotationPresent(type)) {
//                    f.setAccessible(true);
//                    // Init the list
//                    if (injectFields.get(type).get(f.getType()) == null)
//                        injectFields.get(type).put(f.getType(), new ArrayList<>());
//                    injectFields.get(type).get(f.getType()).add(f);
//                }
//            });
//        }
//    }
//
//    public MessageHandlerWrapper newInstance(ForkliftMessage forkliftMessage) throws IllegalAccessException, InstantiationException, InvocationTargetException, IOException {
//
//        final MessageHandlerWrapper[] results = new MessageHandlerWrapper[1];
//        RunAsClassLoader.run(classLoader, () -> {
//            List<Closeable> closeables = new ArrayList<>();
//            try {
//                Object instance = constructMessageHandlerInstance(forkliftMessage, closeables);
//                applyFieldLevelInjection(instance, forkliftMessage, closeables);
//                results[0] = new MessageHandlerWrapper(instance, null);
//            } catch (Exception e) {
//                results[0] = null;
//                log.error("Unable to construct new messageHandler", e);
//            }
//        });
//        return results[0];
//
//    }
//
//    private Object constructMessageHandlerInstance(ForkliftMessage forkliftMessage, List<Closeable> closeables) throws IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
//        Object instance = null;
//        if (this.constructor != null) {
//            Object[] constructorParameters = buildConstructorParameters(forkliftMessage, closeables);
//            this.constructor.newInstance(constructorParameters);
//        } else {
//            instance = messageHandler.newInstance();
//        }
//        return instance;
//    }
//
//    private Object[] buildConstructorParameters(ForkliftMessage forkliftMessage, List<Closeable> closeables) throws IOException {
//
//        Annotation[][] annotations = this.constructor.getParameterAnnotations();
//        Object[] parameters = new Object[annotations.length];
//        int index = 0;
//        for (Annotation[] parameterAnnotations : annotations) {
//            //there must be at least one annotation that indicates what we should inject
//            Annotation injectable = null;
//            for (Annotation parameterAnnotation : parameterAnnotations) {
//                if (injectFields.containsKey(parameterAnnotation)) {
//                    injectable = parameterAnnotation;
//                    break;
//                }
//            }
//            if (injectable == null) {
//                throw new IllegalStateException("Unable to inject " +
//                                                constructor.getParameters()[index].getName() +
//                                                ".  Please check your annotations");
//            } else {
//                Parameter p = constructor.getParameters()[index];
//
//                Object value = getInjectableValue(injectable, p.getName(), p.getClass(), p.getType(), forkliftMessage, closeables);
//                parameters[index] = value;
//            }
//            index++;
//        }
//        return parameters;
//    }
//
//    private void applyFieldLevelInjection(Object instance, ForkliftMessage forkliftMessage, List<Closeable> closeables) {
//        // Inject the forklift msg
//        injectFields.keySet().stream().forEach(decorator -> {
//            final Map<Class<?>, List<Field>> fields = injectFields.get(decorator);
//
//            fields.keySet().stream().forEach(clazz -> {
//                fields.get(clazz).forEach(f -> {
//                    Annotation[] annotations = f.getAnnotations();
//                    for (Annotation annotation : annotations) {
//                        Object value = null;
//                        try {
//                            value = getInjectableValue(annotation, f.getName(), f.getClass(), f.getType(), forkliftMessage, closeables);
//                        } catch (IOException e) {
//                            log.error("Error getting injectableValue", e);
//                        }
//                        if (value != null) {
//                            try {
//                                f.set(instance, value);
//                            } catch (IllegalAccessException e) {
//                                log.error("Error injecting value", e);
//                            }
//                            break;
//                        }
//                    }
//                });
//            });
//        });
//    }
//
//    private Object getInjectableValue(Annotation decorator, String mappedName, Class<?> mappedClass, Type mappedType, ForkliftMessage msg, List<Closeable> closeables) throws IOException {
//        Object value = null;
//        if (decorator.getClass() == forklift.decorators.Message.class && msg.getMsg() != null) {
//            if (mappedClass == ForkliftMessage.class) {
//                value = msg;
//            } else if (mappedClass == String.class) {
//                value = msg.getMsg();
//            } else if (mappedClass == Map.class && !msg.getMsg().startsWith("{")) {
//                value = KeyValueParser.parse(msg.getMsg());
//                // We assume that the map is <String, String>.
//            } else {
//                // Attempt to parse a json
//                value = mapper.readValue(msg.getMsg(), mappedClass);
//            }
//        } else if (decorator.getClass() == javax.inject.Inject.class && this.services != null) {
//            // Try to resolve the class from any available BeanResolvers.
//            for (ConsumerService s : this.services) {
//                try {
//                    final Object o = s.resolve(mappedClass, null);
//                    if (o != null) {
//                        value = o;
//                        break;
//                    }
//                } catch (Exception e) {
//                    log.debug("", e);
//                }
//            }
//        } else if (decorator.getClass() == Config.class) {
//            if (mappedClass == Properties.class) {
//                String confName = ((Config)decorator).value();
//                if (confName.equals("")) {
//                    confName = mappedName;
//                }
//                final Properties config = PropertiesManager.get(confName);
//                if (config == null) {
//                    log.warn("Attempt to inject field failed because resource file {} was not found", ((Config)decorator).value());
//                } else {
//                    value = config;
//                }
//            } else {
//                final Properties config = PropertiesManager.get(((Config)decorator).value());
//                if (config == null) {
//                    log.warn("Attempt to inject field failed because resource file {} was not found", ((Config)decorator).value());
//                }
//                String key = ((Config)decorator).field();
//                if (key.equals("")) {
//                    key = mappedName;
//                }
//                value = config.get(key);
//            }
//        } else if (decorator.getClass() == Headers.class) {
//            final Map<Header, Object> headers = msg.getHeaders();
//            if (mappedClass == Map.class) {
//                value = headers;
//            } else {
//                final Header key = ((Headers)decorator).value();
//                if (headers == null) {
//                    log.warn("Attempt to inject {} from headers, but headers are null", key);
//                } else if (!key.getHeaderType().equals(mappedType)) {
//                    log.warn("Injecting field {} failed because it is not type {}", mappedName, key.getHeaderType());
//                } else {
//                    value = headers.get(key);
//                }
//            }
//        } else if (decorator.getClass() == forklift.decorators.Properties.class) {
//            Map<String, String> properties = msg.getProperties();
//            if (mappedClass == Map.class) {
//                value = msg.getProperties();
//            } else if (properties != null) {
//                String key = ((forklift.decorators.Properties)decorator).value();
//                if (key.equals("")) {
//                    key = mappedName;
//                }
//                if (properties == null) {
//                    log.warn("Attempt to inject field {} from properties, but properties is null", key);
//                } else {
//                    value = properties.get(key);
//                }
//            }
//        } else if (decorator.getClass() == forklift.decorators.Producer.class) {
//            if (mappedClass == ForkliftProducerI.class) {
//                forklift.decorators.Producer producer = (forklift.decorators.Producer)decorator;
//                final ForkliftProducerI p;
//                if (producer.queue().length() > 0)
//                    p = connector.getQueueProducer(producer.queue());
//                else if (producer.topic().length() > 0)
//                    p = connector.getTopicProducer(producer.topic());
//                else
//                    p = null;
//
//                if (p != null)
//                    closeables.add(p);
//                value = p;
//            }
//        }
//        return value;
//    }
//}
