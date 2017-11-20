package forklift.source.decorators;

import forklift.source.SchemaResolver;
import forklift.source.SourceType;
import forklift.source.sources.QueueSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specify which queue a processor should pull messages off.
 */
@Documented
@SourceType(QueueSource.class)
@Repeatable(Queues.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Queue {
    String value();

}
