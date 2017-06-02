package forklift.consumer.injection;

import forklift.decorators.BeanResolver;
import forklift.decorators.Service;

@Service
public class ServiceBeanResolver {

    @BeanResolver
    public Object resolve(Class<?> requestedClass, String name){
       if(requestedClass == Person.class){
           return new Person();
       }
       return null;
    }

}
