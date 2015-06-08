package forklift.notify;

import forklift.consumer.ProcessStep;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instructs forklift's notification engine the step that should trigger events.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface NotifyPost {
    /** The list of steps when to notify. */
    ProcessStep[]steps() default {};

    String url();
}
