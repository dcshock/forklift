package forklift.retry;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instructs forklift how many times a message should be replayed through a processor in the event of an exception.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Retry {
    /**
     *  The number of times a message should be retried. Default is 1
     *
     *  @return number of retries. Default 1
     */
    int maxRetries() default 1;

    /**
     * The amount of time in seconds between retry attempts. Default is 12 hours
     *
     * @return timeout in seconds. Default 12 hrs
     */
    long timeout() default 12 * 60 * 60;

    /**
     * The name of the role to use to write retry messages, if there is no
     * {@link forklift.source.decorators.RoleInput} annotation present on a consumer.
     *
     * @return the name of the fallback role for writing retry messages.
     */
    String role() default "";

    /**
     * Should the message be persisted between retries. Default is true
     *
     * @return true/false whether the message retry is persisted to disk.
     */
    boolean persistent() default true;
}
