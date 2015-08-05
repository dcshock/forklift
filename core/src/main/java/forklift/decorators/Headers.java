package forklift.decorators;

import forklift.message.Header;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a property to have its value populated with the data from the headers of the JMS message
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Headers {
    Header value() default Header.CorrelationId;
}
