package forklift.producers;

interface ForkliftResultResolverI<T> {
    ResultFuture<T> register(ResultFuture<T> future);
    void resolve(String correlationId, T data);
}
