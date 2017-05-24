package forklift.source;

/**
 * A marker for sources which have a specific algorithmic meaning that must be resolved at
 * the connector level.
 *
 * <p>For instance, a named queue should have the properties of giving items in FIFO order
 * and only delivering each item once, and by virtue of requiring those specific properties
 * should be an {@code ActionSource}.
 */
public abstract class ActionSource extends SourceI {
    @Override
    public boolean isLogicalSource() {
        return false;
    }
}
