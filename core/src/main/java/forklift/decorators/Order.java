package forklift.decorators;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instructs forklift to process messages in a specified order. The annotation is placed
 * on a method that returns a unique id for messages that cannot be executed simultaneously.
 * Note that this does not cause the queue to be reordered, but rather insists that messages
 * with the same identifier cannot be executed in separate threads.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Order {
}
