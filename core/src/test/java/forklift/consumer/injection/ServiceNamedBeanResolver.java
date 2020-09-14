package forklift.consumer.injection;

import java.util.HashMap;
import java.util.Map;

import forklift.decorators.BeanResolver;
import forklift.decorators.Service;

@Service
public class ServiceNamedBeanResolver {
    static Map<String, Person> map = new HashMap<>();

    @BeanResolver
    public Object resolve(Class<?> requestedClass, String name){
       if (requestedClass == Person.class && name == null) {
           return new Person();
       } else if (requestedClass == Person.class) {
           return map.computeIfAbsent(name, k -> new Person());
       }
       return null;
    }

}
