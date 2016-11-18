package forklift.consumer;

import forklift.Forklift;
import forklift.classloader.CoreClassLoaders;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Executors;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.deployment.Deployment;
import forklift.deployment.FileDeployment;
import forklift.deployment.DeploymentEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);

    private final Map<Deployment, List<ConsumerThread>> deployments;
    private final Map<Deployment, List<ConsumerService>> serviceDeployments;
    private final Forklift forklift;
    private final ExecutorService executor;

    public ConsumerDeploymentEvents(Forklift forklift, ExecutorService executor) {
        this.deployments = new HashMap<>();
        this.serviceDeployments = new HashMap<>();
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
        final List<ConsumerService> services = new ArrayList<>();

        // Start services by instantiating them. 
        RunAsClassLoader.run(deployment.getClassLoader(), () -> {
            deployment.getServices().forEach(s -> {
                try {
                    log.info("Starting service {}", s);
                    final ConsumerService service = new ConsumerService(s);
                    service.onDeploy();
                    services.add(service);
                } catch (Exception e) {
                    log.error("", e);
                    return;
                }
            });
        });

        // TODO initialize core services, and join classloaders.
        // CoreClassLoaders.getInstance().register(deployment.getClassLoader());

        deployment.getQueues().forEach(c -> {
            for (Annotation a : c.getAnnotationsByType(Queue.class)) {
                log.info("Found annotation {} on {}", a, c);
                final Consumer consumer = new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), (Queue)a); 
                consumer.setServices(services);
                
                final ConsumerThread thread = new ConsumerThread(consumer);
                threads.add(thread);
                executor.submit(thread);    
            }
        });

        deployment.getTopics().forEach(c -> {
            for (Annotation a : c.getAnnotationsByType(Topic.class)) {
                log.info("Found annotation {} on {}", a, c);
                final Consumer consumer = new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), (Topic)a); 
                consumer.setServices(services);

                final ConsumerThread thread = new ConsumerThread(consumer);
                threads.add(thread);
                executor.submit(thread);    
            }
        });

        deployments.put(deployment, threads);
        serviceDeployments.put(deployment, services);
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

        // Shutdown each of the services by calling all of their undeployment methods.
        final List<ConsumerService> services = serviceDeployments.remove(deployment);
        if (services != null && !services.isEmpty()) {
            services.forEach(s -> {
                try {
                    s.onUndeploy();
                } catch (Exception e) {
                    log.warn("", e);
                }
            });
        }

        CoreClassLoaders.getInstance().unregister(deployment.getClassLoader());
    }
}
