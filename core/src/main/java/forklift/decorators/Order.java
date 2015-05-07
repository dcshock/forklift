package forklift.decorators;

import forklift.message.Header;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Instructs forklift to process messages in a specified order. Order can be assigned 
 * by a key in the message body, a standard header key, or a property key. The default
 * is by {@link Header.CorrelationId}.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Order {
	/** The key in the message body in which to order */
	String value() default "";
	/** The header in which to order */ 
	Header header() default Header.CorrelationId;
	/** The property in which to order */
	String property() default "";
}
