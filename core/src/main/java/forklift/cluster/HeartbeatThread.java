package forklift.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.producers.ProducerException;
import forklift.producers.ForkliftProducerI;

import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatThread extends Thread {
    private static Logger log = LoggerFactory.getLogger("HeartbeatThread");
    private static final long timeout = 5000;
    public static CoordinationState state;
    private AtomicBoolean running;
    private ForkliftProducerI producer;

    public HeartbeatThread(String name, ForkliftProducerI producer) {
        super("forklift.heartbeat");
        state = new CoordinationState(name);
        state.getGroups().add(name.substring(0,4));
        this.running = new AtomicBoolean(false);
        this.producer = producer;
        this.setDaemon(true);
    }

    @Override
    public void run() {
        this.running.set(true);
        while (running.get()) {
            try {
                this.producer.send(state);
            } catch (ProducerException e) {
                log.error("Unable to send state", e);
            }
            synchronized (state) {
                try {
                    state.wait(timeout);
                } catch (InterruptedException e) {
                    log.error("InterruptException encountered", e);
                }
            }
        }
    }

    public void shutdown() {
        this.running.set(false);
        synchronized (state) {
            this.notify();
        }

        try {
            this.producer.send(state);
        } catch (ProducerException ignored) { }
    }
}
