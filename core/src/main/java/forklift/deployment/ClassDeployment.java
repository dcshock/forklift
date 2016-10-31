package forklift.deployment;

import forklift.decorators.*;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by afrieze on 10/28/16.
 */
public class ClassDeployment implements Deployment {

    private Set<Class<?>> queues = new HashSet<>();
    private Set<Class<?>> topics = new HashSet<>();
    private Set<Class<?>> services = new HashSet<>();
    private Set<Class<?>> coreServices = new HashSet<>();


    public ClassDeployment(Class<?> ...deploymentClasses){
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
        return null;
    }

    @Override
    public Set<Class<?>> getServices() {
        return null;
    }

    @Override
    public Set<Class<?>> getQueues() {
        return null;
    }

    @Override
    public Set<Class<?>> getTopics() {
        return null;
    }

    @Override
    public ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
    }

    @Override
    public boolean equals(Object other){
        if(!(other instanceof ClassDeployment)){
            return false;
        }
        ClassDeployment deployment = (ClassDeployment)other;

        return false;
    }
}
