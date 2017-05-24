package forklift.deployment;

import forklift.consumer.ConsumerService;

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
     * Gives the set of {@link forklift.consumer.ConsumerService services} that should be deployed with the
     * classes in this {@link Deployment deployment}. These may be automatically derived from classes annotated
     * with the {@link forklift.decorators.Service} annotation.
     *
     * @return the ConsumerServices to be used in this deployment
     */
    Set<ConsumerService> getServices();

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
