package forklift.source;

/**
 * An interface for a context which can resolve a {@link LogicalSource}
 * to a {@link ActionSource} which can be used directly by a connector.
 *
 * <p>When a logical source is resolved into an action source, the
 * source <it>may</it> pass itself into {@link #mapSource(LogicalSource)}
 * if connector information or support is needed in order to perform the
 * resolution to an action source.
 */
public interface LogicalSourceContext {
    ActionSource mapSource(LogicalSource source);
}
