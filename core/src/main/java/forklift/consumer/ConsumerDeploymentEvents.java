package forklift.consumer;

import forklift.Forklift;
import forklift.classloader.CoreClassLoaders;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Executors;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);

    // private final Map<Deployment, ConsumerService> coreServices;
    // private final Map<Deployment, ConsumerService> services;
    private final Map<Deployment, List<ConsumerThread>> deployments;
    private final Forklift forklift;
    private final ExecutorService executor;

    public ConsumerDeploymentEvents(Forklift forklift, ExecutorService executor) {
        this.deployments = new HashMap<>();
        this.forklift = forklift;
        this.executor = executor;
    }

    public ConsumerDeploymentEvents(Forklift forklift) {
        this(forklift, Executors.newCoreThreadPool("consumer-deployment-events"));
    }

    @Override
    public synchronized void onDeploy(final Deployment deployment) {
        log.info("Deploying: " + deployment);

        final List<ConsumerThread> threads = new ArrayList<>();

        // Start services by instantiating them. 
        deployment.getServices().forEach(s -> {
            try {
                s.newInstance();
            } catch (Exception e) {

            }
        });

        // TODO initialize core services, and join classloaders.
        // CoreClassLoaders.getInstance().register(deployment.getClassLoader());

        deployment.getQueues().forEach(c -> {
            for (Annotation a : c.getAnnotationsByType(Queue.class)) {
                log.info("Found annotation {} on {}", a, c);
                final ConsumerThread thread = new ConsumerThread(
                    new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), (Queue)a));
                threads.add(thread);
                executor.submit(thread);    
            }
        });

        deployment.getTopics().forEach(c -> {
            for (Annotation a : c.getAnnotationsByType(Topic.class)) {
                final ConsumerThread thread = new ConsumerThread(
                    new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), (Topic)a));
                threads.add(thread);
                executor.submit(thread);    
            }
        });

        deployments.put(deployment, threads);
    }

    @Override
    public synchronized void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);

        final List<ConsumerThread> threads = deployments.remove(deployment);
        if (threads != null && !threads.isEmpty()) {
            threads.forEach(t -> {
                t.shutdown();
                try {
                    t.join(60000);
                } catch (Exception e) {
                }
            });
        }

        CoreClassLoaders.getInstance().unregister(deployment.getClassLoader());
    }

    /**
     * We allow jar/zip files.
     * @param  deployment
     * @return
     */
    @Override
    public boolean filter(Deployment deployment) {
        log.info("Filtering: " + deployment);

        return deployment.getDeployedFile().getName().endsWith(".jar") ||
               deployment.getDeployedFile().getName().endsWith(".zip");
    }
}
