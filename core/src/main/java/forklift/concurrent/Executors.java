package forklift.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forklift core thread pool.
 * @author zdavep
 */
public final class Executors {

    // Base value for determining thread pool size.
    private static final int cpus = Runtime.getRuntime().availableProcessors();

    /**
     * Disable instance creation.
     */
    private Executors() {}

    /**
     * Allows us to create daemon threads with meaningful names.
     * @param name of the thread
     * @return new ThreadFactory with named thread
     */
    private static ThreadFactory daemonThreadFactory(final String name) {
        return new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            @Override public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r);
                thread.setName(name + "-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
    }

    /**
     *  A core thread pool factory method that returns a cached thread pool.
     * @param name of the thread to create
     * @return a new executor containing the named thread
     */
    public static ExecutorService newCoreThreadPool(final String name) {
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(
            (2 * cpus),  Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), daemonThreadFactory(name)
        );
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return pool;
    }

}
