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

    MessageRunnable(Message jmsMsg, ClassLoader classLoader, Object handler, List<Method> onMessage, List<Method> onValidate) {
        this.jmsMsg = jmsMsg;
    	this.classLoader = classLoader;
    	if (this.classLoader == null)
    		this.classLoader = Thread.currentThread().getContextClassLoader();
    	
        this.handler = handler;
        this.onMessage = onMessage;
        this.onValidate = onValidate;
    }

    @Override
    public void run() {
    	RunAsClassLoader.run(classLoader, () -> {
            try {
                boolean error = false;
                final List<String> allErrors = new ArrayList<>();
                try {
                    // Validate the class.
                    for (Method m : onValidate) {
                        if (m.getReturnType() == List.class) {
                            allErrors.addAll((List<String>)m.invoke(handler));
                        } else if (m.getReturnType() == Boolean.class) {
                            error = error || !((Boolean)m.invoke(handler)).booleanValue();
                        } else {
                            allErrors.add("Return type of " + m.getReturnType() + " is not supported for OnValidate methods");
                            error = true;
                        }

                        if (error || allErrors.size() > 0) 
                            error = true;
                    }

                    // Run the message if there are no errors.
                    if (!error) {    
                        for (Method m : onMessage) {
                            // Send the message to each handler.
      	                    m.invoke(handler, new Object[] {});
            	        }	
                    }
                } catch (Throwable e) {
                    error = true;
                    allErrors.add(e.getMessage());
                }

                if (error) {
                    // TODO audit all errors
                }
            } finally {
                // We've done all we can do to process this message, ack it from the queue, and move forward. 
                try {
                    jmsMsg.acknowledge();
                } catch (JMSException e) {
                }
            }	
    	});
    }
}
