package forklift.consumer;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import forklift.decorators.Queue;
import forklift.decorators.Topic;

public class Consumer {
    private AtomicBoolean running = new AtomicBoolean(false);
    private Set<Class<?>> queues = new HashSet<Class<?>>();
    private Set<Class<?>> topics = new HashSet<Class<?>>();
    
    private File deployedFile;
    private Reflections reflections;

    public Consumer(File deployedFile) 
      throws MalformedURLException {
        this.deployedFile = deployedFile;

        reflections = new Reflections(
            new ConfigurationBuilder()
                .setUrls(new URL[] {deployedFile.toURI().toURL()}));
        
        queues.addAll(reflections.getTypesAnnotatedWith(Queue.class));
        topics.addAll(reflections.getTypesAnnotatedWith(Topic.class));
    }
    
    public boolean isJar() {
        return deployedFile.getPath().endsWith(".jar");
    }
    
    public boolean isClass() {
        return deployedFile.getPath().endsWith(".class");
    }
}
