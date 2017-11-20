package forklift.source.decorators;

import forklift.source.SourceType;
import forklift.source.sources.GroupedTopicSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents a VirtualTopic-like source, where only one consumer from each consumer group will receive a message sent the specified topic.
 */
@Documented
@SourceType(GroupedTopicSource.class)
@Repeatable(GroupedTopics.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GroupedTopic {
    /**
     * The name of the topic being watched.
     */
    String name();

    /**
     * The name of the consumer group reading this topic.
     * <p>
     * If empty, some consumer group name should be generated.
     */
    String group() default "";

}
