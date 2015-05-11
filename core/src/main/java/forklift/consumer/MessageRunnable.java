package forklift.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;

import forklift.classloader.RunAsClassLoader;

public class MessageRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MessageRunnable.class);

    private Message jmsMsg;
	private ClassLoader classLoader;
    private Object handler;
    private List<Method> onMessage;
    private List<Method> onValidate;
    private List<String> errors;
    private boolean error = false;

    MessageRunnable(Message jmsMsg, ClassLoader classLoader, Object handler, List<Method> onMessage, List<Method> onValidate) {
        this.jmsMsg = jmsMsg;
        this.classLoader = classLoader;
        if (this.classLoader == null)
            this.classLoader = Thread.currentThread().getContextClassLoader();
        
        this.handler = handler;
        this.onMessage = onMessage;
        this.onValidate = onValidate;
        this.errors = new ArrayList<>();

        LifeCycleMonitors.call(ProcessStep.Pending, this);
    }

    @Override
    public void run() {
        RunAsClassLoader.run(classLoader, () -> {
            try {
                try {
                    // Validate the class.
                    LifeCycleMonitors.call(ProcessStep.Validating, this);
                    for (Method m : onValidate) {
                        if (m.getReturnType() == List.class) {
                            addError((List<String>)m.invoke(handler));
                        } else if (m.getReturnType() == Boolean.class) {
                            error = error || !((Boolean)m.invoke(handler)).booleanValue();
                        } else {
                            addError("Return type of " + m.getReturnType() + " is not supported for OnValidate methods");
                        }
                    }

                    // Run the message if there are no errors.
                    if (error) {
                        LifeCycleMonitors.call(ProcessStep.Invalid, this);
                    } else {
                        LifeCycleMonitors.call(ProcessStep.Processing, this);
                        for (Method m : onMessage) {
                            // Send the message to each handler.
                            m.invoke(handler, new Object[] {});
                        }   
                    }
                } catch (Throwable e) {
                    addError(e.getMessage());
                }
            } finally {
                // We've done all we can do to process this message, ack it from the queue, and move forward. 
                try {
                    if (error) {
                        LifeCycleMonitors.call(ProcessStep.Error, this);
                    } else {
                        LifeCycleMonitors.call(ProcessStep.Complete, this);   
                    }

                    jmsMsg.acknowledge();
                } catch (JMSException e) {
                }
            }	
    	});
    }

    public void addError(List<String> errors) {
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
}
