package forklift.concurrent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingRejectedExecutionHandlerTests {
    private ThreadPoolExecutor pool;
    private BlockingQueue<Runnable> queue;
    private RejectedExecutionHandler handler;

    @BeforeEach
    public void setup() {
        this.queue = new ArrayBlockingQueue<>(1);
        this.pool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.MILLISECONDS, this.queue);
        this.handler = Mockito.spy(new BlockingRejectedExecutionHandler());
        this.pool.setRejectedExecutionHandler(handler);
    }

    @AfterEach
    public void shutdown() throws Exception {
        pool.shutdownNow();
        pool.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRejectedExecutionTriggered() {
        final AtomicBoolean blockCondition = new AtomicBoolean(false);
        final AtomicBoolean finishCondition = new AtomicBoolean(false);

        // fill up the queue and executing threads - nothing should be rejected yet
        startBlockedTask(blockCondition, pool);
        startBlockedTask(blockCondition, pool);
        Mockito.verify(handler, times(0)).rejectedExecution(any(), any());

        doAsync(() -> {
            pool.execute(() -> {
                synchronized (finishCondition) {
                    finishCondition.set(true);
                    finishCondition.notifyAll();
                }
            });
        });

        // make sure that the third task on the pool doesn't get executed yet, but the rejected execution handler has started
        synchronized (finishCondition) {
            try {
                finishCondition.wait(100);
            } catch (InterruptedException ignored) {}
        }
        assertFalse(finishCondition.get());
        Mockito.verify(handler, times(1)).rejectedExecution(any(), any());

        // unblock the queued threads
        synchronized (blockCondition) {
            blockCondition.set(true);
            blockCondition.notifyAll();
        }

        // check that the queued task eventually gets ran
        synchronized (finishCondition) {
            try {
                finishCondition.wait(100);
            } catch (InterruptedException ignored) {}
        }
        assertTrue(finishCondition.get());
        Mockito.verify(handler, times(1)).rejectedExecution(any(), any());
    }

    private void startBlockedTask(AtomicBoolean condition, ExecutorService executor) {
        executor.submit(() -> {
            while (!condition.get()) {
                synchronized (condition) {
                    try {
                        condition.wait();
                    } catch (InterruptedException ignored) {}
                }
            }
        });
    }

    private void doAsync(Runnable action) {
        final ExecutorService service =  Executors.newSingleThreadExecutor();
        service.execute(action);
        service.shutdown();
    }
}
