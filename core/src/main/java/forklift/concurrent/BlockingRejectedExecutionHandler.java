package forklift.concurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionHandler;

/**
 * A rejected execution handler for a {@link ThreadPoolExecutor} that handles rejected
 * tasks from a running thread pool by blocking.
 */
public class BlockingRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
        while (!executor.isShutdown()) {
            try {
                executor.getQueue().put(runnable);
                return;
            } catch (InterruptedException ignored) {}
        }
    }
}
