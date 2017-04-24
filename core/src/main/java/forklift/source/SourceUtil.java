package forklift.source;

import forklift.decorators.SourceType;
import forklift.decorators.SourceTypeContainer;

import org.reflections.scanners.AbstractScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility for dealing with Source annotations
 */
public class SourceUtil {
    private static final Logger log = LoggerFactory.getLogger(SourceUtil.class);

    public static SourceI fromSourceAnnotation(Annotation annotation) {
        if (!SourceUtil.isSourceAnnotation(annotation))
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

    public static boolean isSourceAnnotation(Annotation annotation) {
        return annotation.annotationType().isAnnotationPresent(SourceType.class);
    }

    public static boolean isSourceAnnotationContainer(Annotation annotation) {
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

                if (SourceUtil.hasSourceAnnotation(clazz)) {
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
