package forklift.deployment;

import java.util.Set;

/**
 * Defines the methods required for a Forklift Deployment.
 *
 * Created by afrieze on 10/28/16.
 */
public interface Deployment {

    /**
     * @return clases in this Deployment annotated with the {@link forklift.decorators.CoreService} annotation
     */
    Set<Class<?>> getCoreServices();

    /**
     * @return clases in this Deployment annotated with the {@link forklift.decorators.Service} annotation
     */
    Set<Class<?>> getServices();

    /**
     * @return clases in this Deployment annotated with the {@link forklift.decorators.Queue} annotation
     */
    Set<Class<?>> getQueues();

    /**
     * @return clases in this Deployment annotated with the {@link forklift.decorators.Topics} annotation
     */
    Set<Class<?>> getTopics();

    /**
     * Returns a {@link ClassLoader} capable of loading the classes encapsulated by this deployment
     *
     * @return {@link ClassLoader}
     */
    ClassLoader getClassLoader();
}
