package forklift.spring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

public class ContextManager {
    private static final String DEFAULT = "default-context";

    private static Map<String, ApplicationContext> contexts =
        new HashMap<String, ApplicationContext>();

    /**
     * Start the default application context with the given configration classes.
     * @param clazzes
     * @return
     */
    public static synchronized ApplicationContext start(Class<?>... clazzes) {
        contexts.put(DEFAULT, new AnnotationConfigApplicationContext(clazzes));
        return getContext(DEFAULT);
    }

    public static synchronized ApplicationContext start(ApplicationContext ctx) {
        contexts.put(DEFAULT, ctx);
        return ctx;
    }

    /**
     * Start an application context with the given configuration classes.
     * @param name
     * @param clazzes
     * @return
     */
    public static synchronized ApplicationContext start(String name, Class<?>... clazzes) {
        contexts.put(name, new AnnotationConfigApplicationContext(clazzes));
        return getContext(name);
    }

    /**
     * Get the default application context.
     * @return
     */
    public static synchronized ApplicationContext getContext() {
        return contexts.get(DEFAULT);
    }

    /**
     * Get an application context by name.
     * @param name
     * @return
     */
    public static synchronized ApplicationContext getContext(String name) {
        return contexts.get(name);
    }

    /**
     * Stop the default context.
     */
    public static synchronized void stop() {
        stop(DEFAULT);
    }

    /**
     * Stop a specific context by name.
     * @param name
     */
    public static synchronized void stop(String name) {
        GenericApplicationContext context = (GenericApplicationContext) contexts.remove(name);

        if (context != null)
            context.close();
    }

    /**
     * Stops all contexts currently being managed.
     */
    public static synchronized void fullStop() {
        final List<String> names = new ArrayList<String>();
        for (String key : contexts.keySet())
            names.add(key);

        for(String name : names)
            stop(name);
    }
}