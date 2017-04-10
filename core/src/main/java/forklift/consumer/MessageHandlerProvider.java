package forklift.consumer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

public class MessageHandlerProvider {

    private final Map<Class, Map<Class<?>, List<Field>>> injectFields = new HashMap<>();
    private final Map<Class, Class<?>> constructorFields = new HashMap<>();
    private Constructor<?> constructor;
    private final Class<?> messageHandlerClass;
    private List<Parameter> constructorParameters = new ArrayList<>();
    private Map<Parameter, Annotation> parameterAnnotations = new HashMap<>();

    public MessageHandlerProvider(Class<?> messageHandlerClass){
        this.messageHandlerClass = messageHandlerClass;
        configureConstructorInjection();
        configureFieldInjection();
    }
    private final void configureConstructorInjection(){
        Constructor<?>[] constructors = this.messageHandlerClass.getDeclaredConstructors();
        for(Constructor<?> constructor : constructors){
            if(constructor.isAnnotationPresent(Inject.class)){
                this.constructor = constructor;
            }
        }
        if(this.constructor != null){
            this.constructor.getParameterAnnotations();
            for(Parameter parameter : this.constructor.getParameters()){

            }
        }
        Annotation[] annotations = this.messageHandlerClass.getAnnotationsByType(Inject.class);

    }

    private final void configureFieldInjection(){

    }

    public Class<?> newInstance(){
       return null;
    }
}
