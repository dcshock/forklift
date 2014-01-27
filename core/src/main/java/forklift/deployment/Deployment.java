package forklift.deployment;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import forklift.classloader.ChildFirstClassLoader;
import forklift.decorators.Queue;
import forklift.decorators.Topic;

public class Deployment {
    private Set<Class<?>> queues = new HashSet<Class<?>>();
    private Set<Class<?>> topics = new HashSet<Class<?>>();
    private ClassLoader cl;
    
    private File deployedFile;
    private Reflections reflections;

    public Deployment() {
        
    }
    
    public Deployment(File deployedFile) 
      throws MalformedURLException {
        this.deployedFile = deployedFile;
        
        final URL[] urls = new URL[] {deployedFile.toURI().toURL()}; 
        
        // Assign a new classloader to this deployment.
        cl = new ChildFirstClassLoader(urls, getClass().getClassLoader());

        // Reflect the deployment to determine if there are any consumers
        // annotated.
        reflections = new Reflections(new ConfigurationBuilder()
            .addClassLoader(cl)
            .setUrls(urls));
        
        queues.addAll(reflections.getTypesAnnotatedWith(Queue.class));
        topics.addAll(reflections.getTypesAnnotatedWith(Topic.class));
    }
    
    public boolean isJar() {
        return deployedFile.getPath().endsWith(".jar");
    }
    
    public boolean isClass() {
        return deployedFile.getPath().endsWith(".class");
    }
    
    public File getDeployedFile() {
        return deployedFile;
    }
    
    public void setDeployedFile(File deployedFile) {
        this.deployedFile = deployedFile;
    }
    
    public ClassLoader getClassLoader() {
        return cl;
    }
    
    public Set<Class<?>> getQueues() {
        return queues;
    }
    
    public Set<Class<?>> getTopics() {
        return topics;
    }
    
    @Override
    public boolean equals(Object o) {
        if (((Deployment)o).getDeployedFile().equals(deployedFile))
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "Deployment [queues=" + queues + ", topics=" + topics + ", cl="
                + cl + ", deployedFile=" + deployedFile + ", reflections="
                + reflections + "]";
    }
}
