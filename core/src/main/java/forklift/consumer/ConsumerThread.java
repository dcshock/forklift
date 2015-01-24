package forklift.consumer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumers need to run in their own thread so that they can spawn more
 * threads to process messages. This also allows the system to isolate 
 * different processes. 
 * @author mconroy
 *
 */
public class ConsumerThread extends Thread {
	private static final AtomicInteger id = new AtomicInteger(0);
	private Consumer consumer;
	
	public ConsumerThread(Consumer consumer) {
		super(consumer.getName() + ":" + id.incrementAndGet());
		this.consumer = consumer;
	}
	
	@Override
	public void run() {
		consumer.listen();
	}
	
	public Consumer getConsumer() {
		return consumer;
	}
}
