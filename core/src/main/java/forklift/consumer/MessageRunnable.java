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
                // { Validating }
                runHooks(ProcessStep.Validating);
                for (Method m : onValidate) {
                    if (m.getReturnType() == List.class) {
                        addError((List<String>)m.invoke(handler));
                    } else if (m.getReturnType() == boolean.class) {
                        if (!(boolean)m.invoke(handler))
                            addError("Validator " + m.getName() + " returned false");
                    } else {
                        throw new RuntimeException("onValidate method " + m.getName() + " has wrong return type " + m.getReturnType());
                    }
                }

                // Run the message if there are no errors.
                if (errors.size() > 0) {
                    // { Invalid }
                    getErrors().stream().forEach(e -> log.error(e));
                    runHooks(ProcessStep.Invalid);
                    // !!EXIT!! if invalid, do not continue
                    return;
                } else {
                    // { Processing }
                    runHooks(ProcessStep.Processing);
                    for (Method m : onMessage) {
                        m.invoke(handler, new Object[] {});
                    }
                }
            } catch (Throwable e) {
                // If we got any errors, log them
                log.info("Error processing", e);
                if (e.getCause() != null)
                    addError(e.getCause().getMessage());
                else
                    addError(e.getMessage());
            }
            // We've done all we can do to process this message, ack it from the queue, and move forward.
            if (errors.size() > 0) {
                // { Error }
                getErrors().stream().forEach(e -> log.error(e));
                try {
                    for (Method m : onProcessStep.get(ProcessStep.Error)) {
                        m.invoke(handler);
                    }
                } catch (Throwable e) {
                    log.error("Error in @On(ProcessStep.Error) handler.", e);
                }
                LifeCycleMonitors.call(ProcessStep.Error, this);
            } else {
                // { Complete }
                try {
                    runHooks(ProcessStep.Complete);
                } catch (Throwable e) {
                    log.error("Error in @On(ProcessStep.Complete) handler.", e);
                    LifeCycleMonitors.call(ProcessStep.Error, this);
                }
            }
            try {
                msg.getJmsMsg().acknowledge();
            } catch (JMSException e) {
                log.error("Error while acking message.", e);
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

    private void runHooks(ProcessStep step) throws Exception {
        for (Method m : onProcessStep.get(step)) {
            m.invoke(handler);
        }
        LifeCycleMonitors.call(step, this);
    }
}
