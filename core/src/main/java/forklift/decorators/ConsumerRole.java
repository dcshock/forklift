package forklift.decorators;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that explicitly gives a name to a consumer to use for identifying it's <it>role</it> or <it>function</it>.
 * <br>
 * That is, this annoation allows a consumer to be identified as performing it's particular function regardless of which
 * server it is on, what sources it processes, or what it is named.
 * <br>
 * If this annotation isn't present, the consuming class's simple name should be taken as name of its role.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ConsumerRole {
    String name();
}
