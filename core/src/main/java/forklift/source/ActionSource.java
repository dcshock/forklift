package forklift.source;

/**
 * An interface for sources which can be used literally with a connector.
 */
public abstract class ActionSource extends SourceI {
    @Override
    public boolean isLogicalSource() {
        return false;
    }
}
