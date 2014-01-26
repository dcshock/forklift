package forklift.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import forklift.decorators.Queue;
import forklift.decorators.Topic;

/**
 * Consumer is responsible for orchestration of moving the message from the broker
 * to the appropriate msgHandler. 
 * @author mattconroy
 *
 */
public class Consumer {
    private Map<String, Listener> listeners = new HashMap<String, Listener>();
    
    public Consumer(Set<Class<?>> msgHandlers) {
        for (Class<?> c : msgHandlers) {
            Queue q = c.getAnnotation(Queue.class);
            Topic t = c.getAnnotation(Topic.class);

            final Listener l = new Listener();
            if (q != null) {
                
            } else if (t != null) {
                
            }
            
            
        }
    }
}
