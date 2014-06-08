package forklift.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

@Component
public class ConsumerManager {
    private AtomicInteger idGen = new AtomicInteger(0);
    private Map<Integer, Consumer> consumers = new HashMap<Integer, Consumer>();

    public synchronized int register(Consumer c) {
        int id = idGen.incrementAndGet();
        consumers.put(id, c);
        return id;
    }

    public synchronized void unregister(int id) {
        consumers.remove(id);
    }
}
