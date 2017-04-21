package forklift.source;

import forklift.decorators.SourceType;
import forklift.decorators.SourceTypeContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;

/**
 * A marker interface for source types
 */
public interface SourceI {
    static final Logger log = LoggerFactory.getLogger(SourceI.class);

    public static SourceI fromSourceAnnotation(Annotation annotation) {
        if (!SourceI.isSourceAnnotation(annotation))
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

    public static boolean isSourceAnnotation(Annotation annotation) {
        return annotation.annotationType().isAnnotationPresent(SourceType.class);
    }

    public static boolean isSourceAnnotationContainer(Annotation annotation) {
        return annotation.annotationType().isAnnotationPresent(SourceTypeContainer.class);
    }
}
