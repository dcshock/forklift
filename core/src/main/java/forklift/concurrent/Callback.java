package forklift.concurrent;

@FunctionalInterface
public interface Callback<V> {
    void handle(V v);
}
