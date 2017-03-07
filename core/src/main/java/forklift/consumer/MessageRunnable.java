package forklift.consumer;

import forklift.classloader.RunAsClassLoader;
import forklift.connectors.ForkliftMessage;
import forklift.producers.ForkliftProducerI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

public class MessageRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MessageRunnable.class);
    private static final String RESPONSE = "@ResponseUri";

    private final Consumer consumer;
    private ForkliftMessage msg;
    private ClassLoader classLoader;
    private Object handler;
    private List<Method> onMessage;
    private List<Method> onValidate;
    private List<Method> onResponse;
    private Map<ProcessStep, List<Method>> onProcessStep;
    private List<String> errors;
    private List<Closeable> closeMe;
    private boolean warnOnly = false;

    MessageRunnable(Consumer consumer, ForkliftMessage msg, ClassLoader classLoader, Object handler, List<Method> onMessage,
                    List<Method> onValidate, List<Method> onResponse, Map<ProcessStep, List<Method>> onProcessStep,
                    List<Closeable> closeMe) {
        this.consumer = consumer;
        this.msg = msg;
        this.classLoader = classLoader;
        if (this.classLoader == null)
            this.classLoader = Thread.currentThread().getContextClassLoader();

        this.handler = handler;
        this.onMessage = onMessage;
        this.onValidate = onValidate;
        this.onResponse = onResponse;
        this.onProcessStep = onProcessStep;
        this.errors = new ArrayList<>();
        this.closeMe = closeMe;

        LifeCycleMonitors.call(ProcessStep.Pending, this);
    }

    @Override
    public void run() {
        RunAsClassLoader.run(classLoader, () -> {
            // Always ack message to prevent activemq deadlock
            try {
                msg.getJmsMsg().acknowledge();
            } catch (JMSException e) {
                //Error code specific to a plugin.  Necessary while we are using
                //the JMS message in order to reduce spamming errors which are actually expected behavior from kafka.
                if("KAFKA-REBALANCE".equals(e.getErrorCode())){
                    log.warn("TopicPartition no longer available, not processing message.");
                }
                else {
                    log.error("Error while acking message.", e);
                }
                close();
                return;
            } catch(Throwable e){
                log.error("Exception", e);
            }

            // { Validating }
            runHooks(ProcessStep.Validating);
            LifeCycleMonitors.call(ProcessStep.Validating, this);
            for (Method m : onValidate) {
                if (m.getReturnType() == List.class) {
                    addError(runLoggingErrors(() -> (List<String>)m.invoke(handler)));
                } else if (m.getReturnType() == boolean.class) {
                    boolean valid = runLoggingErrors(() -> (boolean)m.invoke(handler));
                    if (!valid)
                        addError("Validator " + m.getName() + " returned false");
                } else {
                    addError("onValidate method " + m.getName() + " has wrong return type " + m.getReturnType());
                }
            }

            if (errors.size() > 0) {
                // { Invalid }
                runHooks(ProcessStep.Invalid);
                LifeCycleMonitors.call(ProcessStep.Invalid, this);
            } else {
                // { Processing }
                runHooks(ProcessStep.Processing);
                LifeCycleMonitors.call(ProcessStep.Processing, this);
                for (Method m : onMessage) {
                    runLoggingErrors(() -> m.invoke(handler));
                }
                if (errors.size() > 0) {
                    // { Error }
                    runHooks(ProcessStep.Error);
                    LifeCycleMonitors.call(ProcessStep.Error, this);
                } else {
                    // { Complete }
                    runHooks(ProcessStep.Complete);

                    // Handle response decoratored methods.
                    if (msg.getProperties() != null && msg.getProperties().containsKey(RESPONSE)) {
                        try {
                            final URI uri = new URI(msg.getProperties().get(RESPONSE).toString());

                            onResponse.stream().forEach((m) -> {
                                runLoggingErrors(() -> {
                                    final Object obj = m.invoke(handler);

                                    final ForkliftMessage respMsg = new ForkliftMessage();
                                    respMsg.setHeaders(msg.getHeaders());

                                    if (m.getReturnType() == String.class)
                                        respMsg.setMsg(obj.toString());
                                    else
                                        respMsg.setMsg(consumer.mapper.writeValueAsString(obj));
                                    switch (uri.getScheme()) {
                                        case "queue":
                                            try (ForkliftProducerI producer = consumer.getConnector().getQueueProducer(uri.getHost())) {
                                                System.out.println("Sending: " + respMsg.getMsg());
                                                producer.send(respMsg);
                                            }
                                            break;
                                        case "topic":
                                            try (ForkliftProducerI producer = consumer.getConnector().getTopicProducer(uri.getHost())) {
                                                producer.send(respMsg);
                                            }
                                            break;
                                        case "http":
                                            // Fall through to https
                                        case "https":
                                            break;
                                        default:
                                            log.warn("Unable to find mapping for response uri scheme {}", uri.getScheme());
                                            break;
                                    }
                                    return null;
                                });
                            });
                        } catch (Exception e) {
                            log.error("Unable to determine response uri from {}", msg.getProperties().get(RESPONSE), e);
                        }
                    }

                    LifeCycleMonitors.call(ProcessStep.Complete, this);
                }
            }
            // Always log all non-null errors
            if (this.warnOnly)
                getErrors().stream().filter(e -> e != null).forEach(e -> log.warn(e));
            else
                getErrors().stream().filter(e -> e != null).forEach(e -> log.error(e));
            close();
        });
    }

    public void addError(List<String> errors) {
        if (errors == null)
            return;

        this.errors.addAll(errors);
    }

    public void addError(String e) {
        this.errors.add(e);
    }

    public List<String> getErrors() {
        return errors;
    }

    public ForkliftMessage getMsg() {
        return msg;
    }

    public Object getHandler() {
        return handler;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    /**
     * Set logging to warn only. This allows exceptions that would normally be logged as error to be warnings.
     * @param b
     */
    public void setWarnOnly(boolean b) {
        this.warnOnly = b;
    }

    private void close() {
        // Close resources.
        try {
            for (Closeable c : closeMe)
                c.close();
        } catch (IOException e) {
            log.error("Unable to close a resource", e);
        }
    }

    // This interface and method are for wrapping functions that throw errors, logging and swa
    @FunctionalInterface
    private interface DangerousSupplier<T> {
        T get() throws Throwable;
    }
    private <T> T runLoggingErrors(DangerousSupplier<T> func) {
        try {
            return func.get();
        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw)); // stack trace as a string

            if (e.getCause() == null) {
                addError(e.getMessage() + '\n' + sw.toString());
            } else {
                addError(e.getCause().getMessage() + '\n' + sw.toString());
            }

            return null;
        }
    }

    private void runHooks(ProcessStep step) {
        for (Method m : onProcessStep.get(step)) {
            runLoggingErrors(() -> m.invoke(handler));
        }
    }

    public static void main(String args[]) throws Exception {
        URI uri = new URI("queue://duh");
        System.out.println(uri.getScheme());
        System.out.println(uri.getHost());
    }
}
