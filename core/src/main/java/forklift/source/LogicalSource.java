package forklift.source;

/**
 * A marker for sources that are not tied to a particular algorithmic implementation.
 *
 * <p>As a result, the determination of how to actually read from a particular logical
 * source can be split between the source itself and some kind of
 * {@link LogicalSourceContext context}. Currently, the connector serves as the
 * context for mapping a source.
 *
 * <p>A source that depends on <i>what</i> is being received rather than
 * <i>how</i> it is being received is a good candidate for something that makes
 * sense as a {@code LogicalSource}.
 */
public abstract class LogicalSource extends SourceI {
    @Override
    public boolean isLogicalSource() {
        return true;
    }

    /**
     * Resolves this source to an action source, potentially using the given
     * logical source context, if necessary.
     *
     * @param mapper the logical source context that contains this source
     * @return this source, as represented by an action source
     */
    public abstract ActionSource getActionSource(LogicalSourceContext mapper);
}
