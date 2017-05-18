package forklift.replay;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instruct a forklift to automatically send audit messages for a processor to the audit queue.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Replay {
    /**
     * The name of the role to use to write retry messages, if there is no
     * {@link forklift.source.decorators.RoleInput} annotation present on a consumer.
     *
     * @return the name of the fallback role for writing retry messages.
     */
    String role() default "";
}
