package forklift.replay;

import forklift.source.SourceType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instruct a forklift to automatically send audit messages for a processor to the audit queue.
 */
@SourceType(ReplaySource.class)
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Replay {
    String role() default "";
}
