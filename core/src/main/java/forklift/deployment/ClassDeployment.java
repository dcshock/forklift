package forklift.deployment;

import forklift.decorators.CoreService;
import forklift.decorators.Service;
import forklift.source.SourceUtil;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * A deployment composed of classes.  Generally used to specify classes which are part of a deployment at runtime.
 *
 * Created by afrieze on 10/28/16.
 */
public class ClassDeployment implements Deployment {
    private Set<Class<?>> consumers = new HashSet<>();
    private Set<Class<?>> services = new HashSet<>();
    private Set<Class<?>> coreServices = new HashSet<>();

    public ClassDeployment(Class<?> ...deploymentClasses){
        Preconditions.checkNotNull(deploymentClasses);
        for(Class<?> c : deploymentClasses){
            if (c.isAnnotationPresent(CoreService.class)){
                coreServices.add(c);
            }
            if (c.isAnnotationPresent(Service.class)){
                services.add(c);
            }
            if (SourceUtil.hasSourceAnnotation(c)) {
                consumers.add(c);
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
    public Set<Class<?>> getConsumers() {
        return consumers;
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

        if (!consumers.equals(that.consumers)) return false;
        if (!services.equals(that.services)) return false;
        return coreServices.equals(that.coreServices);

    }

    @Override
    public int hashCode() {
        int result = consumers.hashCode();
        result = 31 * result + services.hashCode();
        result = 31 * result + coreServices.hashCode();
        return result;
    }
}
