package forklift.source;

/**
 * Used to mark a source that is generic and can't be used literally with a
 * connector. Thus, to be used they are resolved to an action source.
 */
public abstract class LogicalSource extends SourceI {
    @Override
    public boolean isLogicalSource() {
        return true;
    }

    public abstract ActionSource getActionSource(LogicalSourceContext mapper);
}
