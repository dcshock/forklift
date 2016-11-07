package forklift.deployment;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import forklift.decorators.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * A deployment composed of classes.  Generally used to specify classes which are part of a deployment at runtime.
 *
 * Created by afrieze on 10/28/16.
 */
public class ClassDeployment implements Deployment {

    private Set<Class<?>> queues = new HashSet<>();
    private Set<Class<?>> topics = new HashSet<>();
    private Set<Class<?>> services = new HashSet<>();
    private Set<Class<?>> coreServices = new HashSet<>();

    public ClassDeployment(Class<?> ...deploymentClasses){
        Preconditions.checkNotNull(deploymentClasses);
        for(Class<?> c : deploymentClasses){
            if(c.isAnnotationPresent(CoreService.class)){
                coreServices.add(c);
            }
            if(c.isAnnotationPresent(Queue.class)){
                queues.add(c);
            }
            if(c.isAnnotationPresent(Queues.class)){
                queues.add(c);
            }
            if(c.isAnnotationPresent(Service.class)){
                services.add(c);
            }
            if(c.isAnnotationPresent(Topic.class)){
                topics.add(c);
            }
            if(c.isAnnotationPresent(Topics.class)){
                topics.add(c);
            }
        }
    }

    @Override
    public Set<Class<?>> getCoreServices() {
        return coreServices;
    }

    @Override
    public Set<Class<?>> getServices() {
        return services;
    }

    @Override
    public Set<Class<?>> getQueues() {
        return queues;
    }

    @Override
    public Set<Class<?>> getTopics() {
        return topics;
    }

    @Override
    public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassDeployment that = (ClassDeployment) o;

        if (!queues.equals(that.queues)) return false;
        if (!topics.equals(that.topics)) return false;
        if (!services.equals(that.services)) return false;
        return coreServices.equals(that.coreServices);

    }

    @Override
    public int hashCode() {
        int result = queues.hashCode();
        result = 31 * result + topics.hashCode();
        result = 31 * result + services.hashCode();
        result = 31 * result + coreServices.hashCode();
        return result;
    }

    private boolean identicalSets(Set<?> set1, Set<?> set2){
        if(set1.size() != set2.size()){
            return false;
        }
        return set1.containsAll(set2);
    }
}
