package forklift.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplayStoreThread<T> extends Thread implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(ReplayWriter.class);

    private AtomicBoolean running = new AtomicBoolean(false);
    private BlockingQueue<T> queue = new ArrayBlockingQueue<>(1000);

    public ReplayStoreThread() {
        this.setDaemon(true);
        this.setName("ReplayStoreThread");
        this.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error(t.getName(), e);
            }
        });
    }

    public void put(T t) {
        try {
            queue.put(t);
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    @Override
    public void run() {
        running.set(true);
        try {
            while (running.get()) {
                final T t = queue.poll(2, TimeUnit.SECONDS);
                if (t != null)
                   poll(t);
                else
                    emptyPoll();
            }

            // Drain to the file.
            final List<T> tList = new ArrayList<>();
            queue.drainTo(tList);
            for (T t : tList)
                poll(t);
        } catch (InterruptedException e) {
                e.printStackTrace();
                return;
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        try {
            this.join(20 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void poll(T t) {

    }

    protected void emptyPoll() {

    }
}
