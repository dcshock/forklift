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
     * Gives all of the classes in the deployment that consume a source.
     *
     * @return clases in this Deployment annotated with a {@link forklift.source.SourceType} type annotation
     */
    Set<Class<?>> getConsumers();

    /**
     * Returns a {@link ClassLoader} capable of loading the classes encapsulated by this deployment
     *
     * @return {@link ClassLoader}
     */
    ClassLoader getClassLoader();
}
