package forklift.decorators;

import forklift.source.SourceI;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that the annotated annotation is a ConsumerSource annotation that can be used to get a ForkliftConsumerI from a connector.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface SourceType {
    Class<? extends SourceI> value();
}
