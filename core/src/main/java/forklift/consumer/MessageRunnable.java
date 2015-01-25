package forklift.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import forklift.classloader.RunAsClassLoader;

public class MessageRunnable implements Runnable {
	private ClassLoader classLoader;
    private Object handler;
    private List<Method> onMessage;

    MessageRunnable(ClassLoader classLoader, Object handler, List<Method> onMessage) {
    	this.classLoader = classLoader;
    	if (this.classLoader == null)
    		this.classLoader = Thread.currentThread().getContextClassLoader();
    	
        this.handler = handler;
        this.onMessage = onMessage;
    }

    @Override
    public void run() {
    	RunAsClassLoader.run(classLoader, () -> {
		   for (Method m : onMessage) {
	            // Send the message to each handler.
	            try {
	                m.invoke(handler, new Object[] {});
	            } catch (IllegalArgumentException e) {
	                e.printStackTrace();
	            } catch (IllegalAccessException e) {
	                e.printStackTrace();
	            } catch (InvocationTargetException e) {
	                e.printStackTrace();
	            }

	            // TODO what happens if more than one method handles the message?
	        }		
    	});
     
    }
}
