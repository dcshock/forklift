package forklift.source.decorators;

import forklift.source.SourceType;
import forklift.source.sources.RoleInputSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specify which role a processor should assume for receiving repeat messages.
 * <br>
 * In the case of an unspecified role, the name of the annotated class is used.
 */
@Documented
@SourceType(RoleInputSource.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface RoleInput {
    String role() default "";
}
