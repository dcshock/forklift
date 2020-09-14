package forklift.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for sources generated from annotations annotated with the {@link SourceType} annotation.
 *
 * <p>These sources are instantiated by passing the annotation scanned at runtime into a constructor
 * for the class specified by the {@link SourceType} annotation.
 *
 * <p>For example, if a class is annotated by {@code @Queue("test")}, passing that class into
 * {@link SourceUtil#getSources(Class)}, will return a {@link java.util.stream.Stream} containing
 * a {@link forklift.source.sources.QueueSource} with a name of {@code "test"}. This is done with
 * the following process: <ol>
 * <li>Search all of the sources on the given source</li>
 * <li>When it comes across the {@link forklift.source.decorators.Queue queue annotation},
 *     it will see that annotation is annotated with {@code @SourceType(QueueSource.class)}. This
 *     will cause a new {@link forklift.source.sources.QueueSource} to be created using the
 *     {@link forklift.source.sources.QueueSource#QueueSource(forklift.source.decorators.Queue)}
 *     constructor.</li>
 * </ol>
 *
 * <p>This class also provides a limited form of case handling for sources, so that one source
 * can be handled only for a few particular cases, in way that is somewhat similar to pattern
 * matching case classes in scala.
 *
 * @see SourceUtil
 */
public abstract class SourceI {
    private static final Logger log = LoggerFactory.getLogger(SourceI.class);

    private Class<?> contextClass;
    public void setContextClass(Class<?> contextClass){
        this.contextClass = contextClass;

        onContextSet();
    }

    public Class<?> getContextClass() {
        return contextClass;
    }

    /**
     * This method should be overriden to initialize any state that depends on the class-based context for the source
     */
    protected void onContextSet() {}

    /**
     * @return whether or not this source needs to be resolved to an action source
     */
    public abstract boolean isLogicalSource();

    /*
     * Case class-type logic
     */
    public <SOURCE extends SourceI, OUT, EX extends Throwable> SourceIHandler<OUT> apply(Class<SOURCE> sourceType, ExceptionalFunction<SOURCE, OUT, EX> action) throws EX {
        return new SourceIHandler<OUT>(this).apply(sourceType, action);
    }

    public <SOURCE extends SourceI, EX extends Throwable> SourceIHandler<Void> accept(Class<SOURCE> sourceType, ExceptionalConsumer<SOURCE, EX> action) throws EX {
        return apply(sourceType, source -> {
            action.accept(source);
            return null;
        });
    }

    public static class SourceIHandler<OUT> {
        private SourceI source;
        private OUT result;
        private boolean handled;

        public SourceIHandler(SourceI source) {
            this.source = source;
        }

        public <SOURCE extends SourceI, EX extends Throwable> SourceIHandler<OUT> apply(Class<SOURCE> sourceType, ExceptionalFunction<SOURCE, OUT, EX> action) throws EX {
            if (!handled && sourceType.isInstance(source)) {
                try {
                    SOURCE specificSource = sourceType.cast(source);

                    result = action.apply(specificSource);
                    handled = true;
                } catch (ClassCastException ignored) { // class cast exception should be impossible
                    log.error("Impossible class cast exception; sound the alarms", ignored);
                } catch (Throwable t) {
                    throw t;
                }
            }

            return this;
        }

        public <SOURCE extends SourceI, EX extends Throwable> SourceIHandler<OUT> accept(Class<SOURCE> sourceType, ExceptionalConsumer<SOURCE, EX> action) throws EX {
            return apply(sourceType, source -> {
                action.accept(source);
                return null;
            });
        }

        public OUT get() {
            return result;
        }

        public OUT getOrDefault(OUT defaultValue) {
            if (!handled)
                return defaultValue;
            return result;
        }

        public OUT elseUnsupportedError() {
            if (!handled) {
                throw new RuntimeException("SourceType " + source.getClass().getSimpleName() + " is not supported");
            }
            return result;
        }
    }

    public interface ExceptionalFunction<I, O, E extends Throwable> {
        O apply(I input) throws E;
    }

    public interface ExceptionalConsumer<I, E extends Throwable> {
        void accept(I input) throws E;
    }
}
