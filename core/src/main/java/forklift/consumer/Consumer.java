package forklift.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import forklift.connectors.ForkliftConnectorI;
import forklift.decorators.Queue;
import forklift.decorators.Topic;

/**
 * Consumer is responsible for creating listeners that connect consuming methods to the
 * backing queue.
 * @author mattconroy
 *
 */
public class Consumer {
	private class ConsumerThread {
		private Listener listener;
		private Thread t;
	}
	
	private Map<Class<?>, ConsumerThread> threads = new HashMap<Class<?>, ConsumerThread>();

    public Consumer(ForkliftConnectorI connector, Set<Class<?>> msgHandlers) {
        for (Class<?> c : msgHandlers) {
            Queue q = c.getAnnotation(Queue.class);
            Topic t = c.getAnnotation(Topic.class);

            final Listener l = new Listener(q, t, c, connector);
            
            final ConsumerThread thread = new ConsumerThread();
            threads.put(c, thread);

            thread.listener = l;
            thread.t = new Thread(() -> {
            	thread.listener.listen();
            });
            thread.t.setName(l.getName());
        }
    }

    /**
     * Startup the listeners that have been created for each msgHandler.
     */
    public void start() {
        final Iterator<ConsumerThread> it = threads.values().iterator();
        while (it.hasNext()) 
        	it.next().t.start();
    }

    public void stop() {
        final Iterator<ConsumerThread> it = threads.values().iterator();
        while (it.hasNext())
            it.next().listener.shutdown();
    }

    public Listener getListener(Class<?> clazz) {
        return threads.get(clazz).listener;
    }
    
    public Thread getThread(Listener l) {
    	final Iterator<ConsumerThread> it = threads.values().iterator();
        while (it.hasNext()) {
        	ConsumerThread consumerThread = it.next();
        	if (consumerThread.listener == l)
        		return consumerThread.t;
        }
        return null;
    }
}
