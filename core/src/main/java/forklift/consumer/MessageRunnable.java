package forklift.consumer;

import forklift.classloader.RunAsClassLoader;
import forklift.connectors.ForkliftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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
    private List<Closeable> closeMe;

    MessageRunnable(Consumer consumer, ForkliftMessage msg, ClassLoader classLoader, Object handler, List<Method> onMessage,
                    List<Method> onValidate, Map<ProcessStep, List<Method>> onProcessStep, List<Closeable> closeMe) {
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
        this.closeMe = closeMe;

        LifeCycleMonitors.call(ProcessStep.Pending, this);
    }

    @Override
    public void run() {
        RunAsClassLoader.run(classLoader, () -> {
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
                    LifeCycleMonitors.call(ProcessStep.Complete, this);
                }
            }
            // Always log all errors
            getErrors().stream().forEach(e -> log.error(e));
            // Always ack message to prevent activemq deadlock
            try {
                msg.getJmsMsg().acknowledge();
            } catch (JMSException e) {
                log.error("Error while acking message.", e);
            }

            // Close resources.
            try {
                for (Closeable c : closeMe)
                    c.close();
            } catch (IOException e) {
                log.error("Unable to close a resource", e);
            }
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

    // This interface and method are for wrapping functions that throw errors, logging and swa
    @FunctionalInterface
    private interface DangerousSupplier<T> {
        T get() throws Throwable;
    }
    private <T> T runLoggingErrors(DangerousSupplier<T> func) {
        try {
            return func.get();
        } catch (Throwable e) {
            if (e.getCause() == null) {
                addError(e.getMessage());
            }
            else {
                addError(e.getCause().getMessage());
            }
            return null;
        }
    }

    private void runHooks(ProcessStep step) {
        for (Method m : onProcessStep.get(step)) {
            runLoggingErrors(() -> m.invoke(handler));
        }
    }
}
