package forklift.consumer;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // Hold a lock object to allow for shutdowns during exponential backoff of reconnects.
    private Object lock = new Object();

    // Store a basic exponential backoff sequence.
    private static final long[] expoSeq = {3, 5, 8, 13, 21, 34, 55};

    private AtomicBoolean running;
    private Consumer consumer;

    public ConsumerThread(Consumer consumer) {
        super(consumer.getName() + ":" + id.incrementAndGet());
        this.running = new AtomicBoolean(false);
        this.consumer = consumer;
    }

    @Override
    public void run() {
        running.set(true);

        LocalDate lastConnectAttemptTime;
        int connectAttempt = 0;
        do {
            lastConnectAttemptTime = LocalDate.now();
            consumer.listen();

            synchronized (lock) {
                // If we are still running let's wait a little while and attempt to reconnect.
                if (running.get()) {
                    // Reset connection attempts if we have been connected for longer than the max wait time.
                    if (LocalDate.now().isAfter(lastConnectAttemptTime.plus(expoSeq[expoSeq.length - 1], ChronoUnit.SECONDS)))
                        connectAttempt = 0;


                    try {
                        lock.wait(expoSeq[connectAttempt] * 1000);

                        // Never let the attempt number get bigger than the greatest backoff sequence number.
                        connectAttempt = Math.min(connectAttempt + 1, expoSeq.length - 1);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        } while (running.get());
    }

    public void shutdown() {
        // Jump us out of any exponential backoff wait we might be in.
        synchronized (lock) {
            lock.notify();
            consumer.shutdown();
            running.set(false);
        }
    }
}
