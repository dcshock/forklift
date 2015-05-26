package forklift.decorators;

import forklift.consumer.ProcessStep;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Repeatable(LifeCycles.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface LifeCycle {
    ProcessStep value();
    Class<? extends Annotation> annotation() default Annotation.class;
}