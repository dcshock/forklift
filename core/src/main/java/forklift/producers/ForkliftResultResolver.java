package forklift.producers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ForkliftResultResolver<T> implements ForkliftResultResolverI<T> {
    private Map<String, ResultFuture<T>> futures = new ConcurrentHashMap<>(new HashMap<>());

    @Override
    public ResultFuture<T> register(ResultFuture<T> future) {
        if (future == null)
            return null;

        if (futures.containsKey(future.getCorrelationId()))
            throw new RuntimeException("Duplicate correlation id, unable to register future.");

        futures.put(future.getCorrelationId(), future);
        return future;
    }

    @Override
    public void resolve(String correlationId, T t) {
        final ResultFuture<T> future = futures.remove(correlationId);
        if (future != null)
            future.complete(t);
    }
}
