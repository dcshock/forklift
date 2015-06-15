package forklift.consumer;

import forklift.classloader.RunAsClassLoader;
import forklift.connectors.ForkliftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

public class MessageRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MessageRunnable.class);

    private Consumer consumer;
    private ForkliftMessage msg;
    private ClassLoader classLoader;
    private Object handler;
    private List<Method> onMessage;
    private List<Method> onValidate;
    private Map<ProcessStep, List<Method>> onProcessStep;
    private List<String> errors;
    private boolean error = false;

    MessageRunnable(Consumer consumer, ForkliftMessage msg, ClassLoader classLoader, Object handler, List<Method> onMessage, List<Method> onValidate, Map<ProcessStep, List<Method>> onProcessStep) {
        this.consumer = consumer;
        this.msg = msg;
        this.classLoader = classLoader;
        if (this.classLoader == null)
            this.classLoader = Thread.currentThread().getContextClassLoader();

        this.handler = handler;
        this.onMessage = onMessage;
        this.onValidate = onValidate;
        this.onProcessStep = onProcessStep;
        this.errors = new ArrayList<>();

        LifeCycleMonitors.call(ProcessStep.Pending, this);
    }

    @Override
    public void run() {
        RunAsClassLoader.run(classLoader, () -> {
            try {
                try {
                    // Validate the class.
                    for (Method m : onProcessStep.get(ProcessStep.Validating)) {
                        m.invoke(handler);
                    }
                    LifeCycleMonitors.call(ProcessStep.Validating, this);
                    for (Method m : onValidate) {
                        if (m.getReturnType() == List.class) {
                            addError((List<String>)m.invoke(handler));
                        } else if (m.getReturnType() == boolean.class) {
                            error = error || !((boolean)m.invoke(handler));
                        } else {
                            addError("Return type of " + m.getReturnType() + " is not supported for OnValidate methods");
                        }
                    }

                    // Run the message if there are no errors.
                    if (error) {
                        for (Method m : onProcessStep.get(ProcessStep.Invalid)) {
                            m.invoke(handler);
                        }
                        LifeCycleMonitors.call(ProcessStep.Invalid, this);
                    } else {
                        for (Method m : onProcessStep.get(ProcessStep.Processing)) {
                            m.invoke(handler);
                        }
                        LifeCycleMonitors.call(ProcessStep.Processing, this);
                        // Send the message to each handler.
                        for (Method m : onMessage) {
                            m.invoke(handler, new Object[] {});
                        }
                    }
                } catch (Throwable e) {
                    log.info("Error processing", e);
                    if (e.getCause() != null)
                        addError(e.getCause().getMessage());
                    else
                        addError(e.getMessage());
                }
            } finally {
                // We've done all we can do to process this message, ack it from the queue, and move forward.
                try {
                    if (error) {
                        getErrors().stream().forEach(e -> log.error(e));
                        try {
                            for (Method m : onProcessStep.get(ProcessStep.Error)) {
                                m.invoke(handler);
                            }
                        } catch (Throwable e) {
                            log.info("Error in @On(ProcessStep.Error) handler.", e);
                        }
                        LifeCycleMonitors.call(ProcessStep.Error, this);
                    } else {
                        try {
                            for (Method m : onProcessStep.get(ProcessStep.Complete)) {
                                m.invoke(handler);
                            }
                            LifeCycleMonitors.call(ProcessStep.Complete, this);
                        } catch (Throwable e) {
                            log.info("Error in @On(ProcessStep.Complete) handler.", e);
                            LifeCycleMonitors.call(ProcessStep.Error, this);
                        }
                    }

                    msg.getJmsMsg().acknowledge();
                } catch (JMSException e) {
                    log.error("Error while acking message.", e);
                }
            }
        });
    }

    public void addError(List<String> errors) {
        if (errors == null)
            return;

        this.errors.addAll(errors);

        if (this.errors.size() > 0)
            setError();
    }

    public void addError(String e) {
        this.errors.add(e);
        setError();
    }

    public void setError() {
        this.error = true;
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
}
