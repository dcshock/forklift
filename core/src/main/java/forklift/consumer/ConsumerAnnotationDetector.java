package forklift.consumer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import forklift.decorators.Config;
import forklift.decorators.On;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Ons;
import forklift.decorators.Order;
import forklift.decorators.Response;

public class ConsumerAnnotationDetector {
    private final Map<Class, Map<Class<?>, List<Field>>> injectFields;
    private final Class<?> msgHandler;
    private final List<Method> onMessage;
    private final List<Method> onValidate;
    private final List<Method> onResponse;
    private final Map<String, List<MessageRunnable>> orderQueue;
    private final Map<ProcessStep, List<Method>> onProcessStep;
    private Method orderMethod;

    public ConsumerAnnotationDetector(Class<?> msgHandler, boolean supportsOrder, boolean supportsResponse) {
        this.msgHandler = msgHandler;
        // Look for all methods that need to be called when a
        // message is received.
        onMessage = new ArrayList<>();
        onValidate = new ArrayList<>();
        onResponse = new ArrayList<>();
        onProcessStep = new HashMap<>();
        Arrays.stream(ProcessStep.values()).forEach(step -> onProcessStep.put(step, new ArrayList<>()));
        for (Method m : getAllMethodsInHierarchy(msgHandler)) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(Response.class)) {
                if (!supportsResponse)
                    throw new RuntimeException("@Response is not supported by the current connector");

                onResponse.add(m);
            } else if (m.isAnnotationPresent(Order.class)) {
                if (!supportsOrder)
                    throw new RuntimeException("@Order is not supported by the current connector");

                orderMethod = m;
            } else if (m.isAnnotationPresent(On.class) || m.isAnnotationPresent(Ons.class))
                Arrays.stream(m.getAnnotationsByType(On.class))
                    .map(on -> on.value())
                    .distinct()
                    .forEach(x -> onProcessStep.get(x).add(m));
        }
        if (orderMethod != null)
            orderQueue = new HashMap<>();
        else
            orderQueue = null;

        injectFields = new HashMap<>();
        injectFields.put(Config.class, new HashMap<>());
        injectFields.put(javax.inject.Inject.class, new HashMap<>());
        injectFields.put(forklift.decorators.Message.class, new HashMap<>());
        injectFields.put(forklift.decorators.Headers.class, new HashMap<>());
        injectFields.put(forklift.decorators.Properties.class, new HashMap<>());
        injectFields.put(forklift.decorators.Producer.class, new HashMap<>());
        for (Field f : getAllFieldsInHierarchy(msgHandler)) {
            injectFields.keySet().forEach(type -> {
                if (f.isAnnotationPresent(type)) {
                    f.setAccessible(true);

                    // Init the list
                    if (injectFields.get(type).get(f.getType()) == null)
                        injectFields.get(type).put(f.getType(), new ArrayList<>());
                    injectFields.get(type).get(f.getType()).add(f);
                }
            });
        }
    }

    private static Method[] getAllMethodsInHierarchy(Class<?> objectClass) {
        Set<Method> allMethods = new HashSet<Method>();
        Method[] declaredMethods = objectClass.getDeclaredMethods();
        Method[] methods = objectClass.getMethods();
        if (objectClass.getSuperclass() != null) {
            Class<?> superClass = objectClass.getSuperclass();
            Method[] superClassMethods = getAllMethodsInHierarchy(superClass);
            allMethods.addAll(Arrays.asList(superClassMethods));
        }
        for (Class<?> interf : objectClass.getInterfaces()) {
            Method[] interfaceMethods = getAllMethodsInHierarchy(interf);
            allMethods.addAll(Arrays.asList(interfaceMethods));
        }

        allMethods.addAll(Arrays.asList(declaredMethods));
        allMethods.addAll(Arrays.asList(methods));
        return allMethods.toArray(new Method[allMethods.size()]);
    }

    private static Field[] getAllFieldsInHierarchy(Class<?> objectClass) {
        Set<Field> allfields = new HashSet<Field>();
        Field[] declaredfields = objectClass.getDeclaredFields();
        Field[] fields = objectClass.getFields();
        if (objectClass.getSuperclass() != null) {
            Class<?> superClass = objectClass.getSuperclass();
            Field[] superClassfields = getAllFieldsInHierarchy(superClass);
            allfields.addAll(Arrays.asList(superClassfields));
        }
        allfields.addAll(Arrays.asList(declaredfields));
        allfields.addAll(Arrays.asList(fields));
        return allfields.toArray(new Field[allfields.size()]);
    }
    public Map<Class, Map<Class<?>, List<Field>>> getInjectFields() {
        return injectFields;
    }

    public Class<?> getMsgHandler() {
        return msgHandler;
    }

    public List<Method> getOnMessage() {
        return onMessage;
    }

    public List<Method> getOnValidate() {
        return onValidate;
    }

    public List<Method> getOnResponse() {
        return onResponse;
    }

    public Map<String, List<MessageRunnable>> getOrderQueue() {
        return orderQueue;
    }

    public Map<ProcessStep, List<Method>> getOnProcessStep() {
        return onProcessStep;
    }

    public Method getOrderMethod() {
        return orderMethod;
    }

}
