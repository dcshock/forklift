package forklift.decorators;

import forklift.consumer.RequiredSystem;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instructs forklift to check that a required system is up and running. Checks the availability
 * of a system before processing messages by a consumer. RequireSystem is supported by system services.
 */
@Documented
@Inherited
@Repeatable(RequireSystems.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface RequireSystem {
    Class<? extends RequiredSystem> value();
    long msTimeout() default 10000L;
}
