package forklift.source.decorators;

import forklift.source.SchemaResolver;
import forklift.source.SourceI;
import forklift.source.SourceType;
import forklift.source.sources.TopicSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specify which Topic a processor should watch for messages.
 */
@Documented
@SourceType(TopicSource.class)
@Repeatable(Topics.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Topic {
    String value();
//    Class<? extends SchemaResolver> schemaResolver() default None.class;
//
//    //because you can't use null as a default value in an annotation.
//    class None implements SchemaResolver {
//        private static final long serialVersionUID = 1L;
//
//        private None() {
//        }
//
//        @Override
//        public String getSchema(SourceI source) {
//            return null;
//        }
//    }

}
