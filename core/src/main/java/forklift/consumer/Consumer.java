package forklift.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import forklift.connectors.ForkliftConnectorI;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.spring.ContextManager;

/**
 * Consumer is responsible for creating listeners to connect consuming methods to the
 * backing queue.
 * @author mattconroy
 *
 */
public class Consumer {
    private Map<Class<?>, Listener> listeners = new HashMap<Class<?>, Listener>();

    public Consumer(Set<Class<?>> msgHandlers) {
        for (Class<?> c : msgHandlers) {
            Queue q = c.getAnnotation(Queue.class);
            Topic t = c.getAnnotation(Topic.class);

            final Listener l = new Listener(
                q, t, c, ContextManager.getContext().getBean(ForkliftConnectorI.class));
            listeners.put(c, l);
        }
    }

    /**
     * Startup the listeners that have been created for each msgHandler.
     */
    public void start() {
        final Iterator<Listener> it = listeners.values().iterator();
        while (it.hasNext())
            it.next().listen();
    }

    public Listener getListener(Class<?> clazz) {
        return listeners.get(clazz);
    }
}
