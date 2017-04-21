package forklift.connectors;

import forklift.decorators.SourceType;
import forklift.decorators.SourceTypeContainer;
import forklift.source.SourceI;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsumerSource {
    public static List<ConsumerSource> getConsumerSources(Class<?> clazz) {
        return Arrays.stream(clazz.getAnnotations())
            .flatMap(annotation -> {
                if (SourceI.isSourceAnnotation(annotation))
                    return Stream.of(annotation);
                else if (SourceI.isSourceAnnotationContainer(annotation))
                    return Arrays.stream(getContainedAnnotations(annotation));

                return Stream.empty();
             })
            .map(annotation -> SourceI.fromSourceAnnotation(annotation))
            .map(source -> new ConsumerSource(source))
            .collect(Collectors.toList());
    }

    private static Annotation[] getContainedAnnotations(Annotation annotation) {
        try {
            return (Annotation[]) annotation.annotationType().getMethod("value").invoke(annotation);
        } catch (Exception ignored) {}
        return new Annotation[] {};
    }

    private SourceI source;
    public ConsumerSource(SourceI source) {
        this.source = source;
    }

    public SourceI getSource() {
        return source;
    }

    public <SOURCE extends SourceI, OUT, EX extends Throwable> ConsumerSourceHandler<OUT> apply(Class<SOURCE> sourceType, ExceptionalFunction<SOURCE, OUT, EX> action) throws EX {
        return new ConsumerSourceHandler<OUT>().apply(sourceType, action);
    }

    public <SOURCE extends SourceI, EX extends Throwable> ConsumerSourceHandler<Void> accept(Class<SOURCE> sourceType, ExceptionalConsumer<SOURCE, EX> action) throws EX {
        return apply(sourceType, source -> {
            action.accept(source);
            return null;
        });
    }

    @Override
    public String toString() {
        return "ConsumerSource(" + source + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ConsumerSource))
            return false;

        ConsumerSource that = (ConsumerSource) o;
        return Objects.equals(this.source, that.source);
    }

    public class ConsumerSourceHandler<OUT> {
        private OUT result;
        private boolean handled;

        public <SOURCE extends SourceI, EX extends Throwable> ConsumerSourceHandler<OUT> apply(Class<SOURCE> sourceType, ExceptionalFunction<SOURCE, OUT, EX> action) throws EX {
            if (!handled && sourceType.isInstance(source)) {
                try {
                    SOURCE specificConsumerSource = sourceType.cast(source);

                    result = action.apply(specificConsumerSource);
                    handled = true;
                } catch (ClassCastException ignored) { // class cast exception should be impossible
                } catch (Throwable t) {
                    throw t;
                }
            }

            return this;
        }

        public <SOURCE extends SourceI, EX extends Throwable> ConsumerSourceHandler<OUT> accept(Class<SOURCE> sourceType, ExceptionalConsumer<SOURCE, EX> action) throws EX {
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
