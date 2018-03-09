package forklift.concurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionHandler;

/**
 * A rejected execution handler for a {@link ThreadPoolExecutor} that handles rejected
 * tasks from a running thread pool by blocking.
 */
public class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {
    private static final Runnable NO_OP = () -> {};

    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
        if (executor.isShutdown()) { return; }

        try {
            // wait for space in the queue
            executor.getQueue().put(NO_OP);
            executor.remove(NO_OP);

            if (!executor.isShutdown()) {
                executor.execute(runnable);
            }
        } catch (InterruptedException ignored) {}
    }
}
