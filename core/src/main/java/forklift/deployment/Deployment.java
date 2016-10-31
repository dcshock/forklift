package forklift.deployment;

import java.util.Set;

/**
 * Created by afrieze on 10/28/16.
 */
public interface Deployment {

    Set<Class<?>> getCoreServices();

    Set<Class<?>> getServices();

    Set<Class<?>> getQueues();

    Set<Class<?>> getTopics();

    /**
     * Returns a {@link ClassLoader} capable of loading the classes encapsulated by this deployment
     *
     * @return {@link ClassLoader}
     */
    ClassLoader getClassLoader();
}
