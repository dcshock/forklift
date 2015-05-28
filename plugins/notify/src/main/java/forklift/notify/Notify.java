package forklift.notify;

import forklift.consumer.ProcessStep;
import forklift.message.Header;

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
public @interface Notify {
    /** The list of steps when to notify. */
    ProcessStep[] steps() default {};
    /** The message keys and values within the message body to be sent in the notification message. */
    String[] values() default {};
    /** The message headers to be sent in the notification message. */
    Header[] headers() default { Header.CorrelationId };
    /** The message properties to be sent in the notification message. */
    String[] properties() default {};
    /** Override values, headers, and properties and send everything from the original message with the notification. */
    boolean fullMessage() default true;
}