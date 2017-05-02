package forklift.source;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A superclass for {@link forklift.source.SourceType} annotations that provides some
 * case handling
 */
public abstract class SourceI {
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
