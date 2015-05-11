package forklift.consumer;

import org.springframework.context.annotation.Configuration;

public class ConsumerService {
    private boolean spring;
    private Class<?> clazz;

    public ConsumerService(Class<?> clazz) {
        this.clazz = clazz;
        this.spring = this.clazz.isAnnotationPresent(Configuration.class);


    }
}