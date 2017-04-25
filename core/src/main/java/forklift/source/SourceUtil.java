package forklift.source;

import forklift.source.SourceType;
import forklift.source.SourceTypeContainer;

import org.reflections.scanners.AbstractScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility for dealing with Source annotations
 */
public class SourceUtil {
    private static final Logger log = LoggerFactory.getLogger(SourceUtil.class);

    public static Stream<SourceI> getSources(Class<?> clazz) {
        return Arrays.stream(clazz.getAnnotations())
            .flatMap(annotation -> {
                if (isSourceAnnotation(annotation))
                    return Stream.of(annotation);
                else if (isSourceAnnotationContainer(annotation))
                    return Arrays.stream(getContainedAnnotations(annotation));

                return Stream.empty();
             })
            .map(annotation -> {
                SourceI source = fromSourceAnnotation(annotation);
                source.setContextClass(clazz);
                return source;
            });
    }

    public static List<SourceI> getSourcesAsList(Class<?> clazz) {
        return Collections.unmodifiableList(
            SourceUtil.getSources(clazz)
                .collect(Collectors.toList()));
    }

    public static <SOURCE extends SourceI> Stream<SOURCE> getSources(Class<?> clazz, Class<SOURCE> sourceType) {
        return getSources(clazz)
                .filter(source -> sourceType.isInstance(source))
                .map(source -> {
                     try {
                         return sourceType.cast(source);
                     } catch (ClassCastException e) { // a class cast exception should be impossible
                         return null;
                     }
                });
    }

    private static Annotation[] getContainedAnnotations(Annotation annotation) {
        try {
            return (Annotation[]) annotation.annotationType().getMethod("value").invoke(annotation);
        } catch (Exception ignored) {}
        return new Annotation[] {};
    }

    public static SourceI fromSourceAnnotation(Annotation annotation) {
        if (!isSourceAnnotation(annotation))
            return null;

        try {
            SourceType sourceType = annotation.annotationType().getAnnotation(SourceType.class);
            Class<? extends SourceI> sourceClass = sourceType.value();

            Constructor<? extends SourceI> sourceConstructor = sourceClass.getConstructor(annotation.annotationType());
            return (SourceI) sourceConstructor.newInstance(annotation);
        } catch (Exception e) {
            log.error("Could not instantiate source", e);
        }
        return null;
    }

    public static boolean hasSourceAnnotation(Class<?> clazz) {
        return Arrays.asList(clazz.getAnnotations()).stream()
            .anyMatch(annotation -> isSourceAnnotation(annotation) || isSourceAnnotationContainer(annotation));
    }

    private static boolean isSourceAnnotation(Annotation annotation) {
        return annotation.annotationType().isAnnotationPresent(SourceType.class);
    }

    private static boolean isSourceAnnotationContainer(Annotation annotation) {
        return annotation.annotationType().isAnnotationPresent(SourceTypeContainer.class);
    }

    /**
     * A scanner to work well-enough with the reflections library
     */
    public static class SourceTypeScanner extends AbstractScanner {
        private Set<Class<?>> classesFound = new HashSet<>();
        private ClassLoader loader;

        public SourceTypeScanner(ClassLoader loader) {
            this.loader = loader;
        }

        @Override
        public void scan(final Object cls) {
            final String className = getMetadataAdapter().getClassName(cls);
            try {
                Class<?> clazz = loader.loadClass(className);

                if (hasSourceAnnotation(clazz)) {
                    classesFound.add(clazz);
                }
            } catch (ClassNotFoundException e) {
                log.debug("Could not find class while scanning: " + className, e);
            }
        }

        public Set<Class<?>> getSourceAnnotatedTypes() {
            return classesFound;
        }
    }
}
