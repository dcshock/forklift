package forklift.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import forklift.connectors.ForkliftConnectorI;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.spring.ContextManager;

/**
 * Consumer is responsible for orchestration of moving the message from the broker
 * to the appropriate msgHandler.
 * @author mattconroy
 *
 */
public class Consumer {
    private Integer id = null;
    private Map<String, Listener> listeners = new HashMap<String, Listener>();

    public Consumer(Set<Class<?>> msgHandlers) {
        for (Class<?> c : msgHandlers) {
            Queue q = c.getAnnotation(Queue.class);
            Topic t = c.getAnnotation(Topic.class);

            final Listener l = new Listener(
                q, t, c, ContextManager.getContext().getBean(ForkliftConnectorI.class));
            listeners.put(l.getName(), l);
            l.listen();
        }
    }

    void setId(Integer id) {
        this.id = id;
    }
}
