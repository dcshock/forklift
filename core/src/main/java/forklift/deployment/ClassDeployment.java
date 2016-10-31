package forklift.deployment;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import forklift.decorators.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
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
    public int hashCode(){
        int result = 0;
        for(Class<?> c : Iterables.concat(coreServices,queues,services,topics)){
            result += c.getName().hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object other){
        if(!(other instanceof ClassDeployment)){
            return false;
        }
        ClassDeployment d = (ClassDeployment)other;
        boolean equals = identicalSets(this.coreServices, d.coreServices);
        equals = equals ? identicalSets(this.services, d.services):false;
        equals = equals ? identicalSets(this.queues, d.queues):false;
        equals = equals ? identicalSets(this.topics, d.topics):false;
        return equals;
    }

    private boolean identicalSets(Set<?> set1, Set<?> set2){
        if(set1.size() != set2.size()){
            return false;
        }
        return set1.containsAll(set2);
    }
}
