package forklift.source;

/**
 * An object which can map a logical source to an action source.
 */
public interface LogicalSourceContext {
    ActionSource mapSource(LogicalSource source);
}
