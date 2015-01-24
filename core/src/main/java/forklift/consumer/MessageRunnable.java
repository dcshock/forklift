package forklift.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MessageRunnable implements Runnable {
    private Object handler;
    private List<Method> onMessage;

    MessageRunnable(Object handler, List<Method> onMessage) {
        this.handler = handler;
        this.onMessage = onMessage;
    }

    @Override
    public void run() {
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
    }

}
