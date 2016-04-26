package forklift.producers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResultFuture<T> extends CompletableFuture<T> {
    private String correlationId;

    public ResultFuture(String correlationId) {
        super();
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

    public String getCorrelationId() {
        return correlationId;
    }
}
