package forklift.producers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResultFuture<T> extends CompletableFuture<T> {
    private AtomicBoolean done = new AtomicBoolean(false);

    private T t;
    private String correlationId;

    public ResultFuture(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Cannot be cancelled due to the nature of JMS being fire and forget.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    /**
     * Cannot be cancelled due to the nature of JMS being fire and forget.
     */
    @Override
    public boolean isCancelled() {
        return false;
    }

    /**
     * Does this future have a result.
     */
    @Override
    public boolean isDone() {
        return done.get();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (done) {
            if (!done.get())
                done.wait(unit.toMillis(timeout));

            return this.t;
        }
    }

    public void resolve(T t) {
        if (done.get())
            return;

        synchronized (done) {
            this.t = t;
            done.set(true);
            done.notifyAll();
        }
    }

    public String getCorrelationId() {
        return correlationId;
    }
}