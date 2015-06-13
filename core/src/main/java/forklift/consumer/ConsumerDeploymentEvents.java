package forklift.consumer;

import forklift.Forklift;
import forklift.classloader.CoreClassLoaders;
import forklift.classloader.RunAsClassLoader;
import forklift.concurrent.Executors;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;
import forklift.spring.ContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

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

        // Launch a Spring context if necessary. Note that we ensure that spring has access to the deployment's classloader since the
        // configurations will be there. We'll also need to be careful not to utilize spring in the forklift core since the system may
        // spin up classes more than once. Never add a config with a scan that looks at forklift.*.
        // TODO - Somehow fix the ability to use spring in the core safely - maybe a check to ensure forklift.* isn't scanned in a  consumer deployment.
        RunAsClassLoader.run(deployment.getClassLoader(), () -> {
            final List<Class<?>> springConfigs = deployment.getReflections().getTypesAnnotatedWith(Configuration.class).stream().collect(Collectors.toList());
            if (springConfigs.size() > 0) {
                final Class<?>[] clazzes = new Class<?>[springConfigs.size()];
                for (int i = 0; i < clazzes.length; i++)
                    clazzes[i] = springConfigs.get(i);

                ContextManager.start(deployment.getDeployedFile().getName(), clazzes);
            }
        });
        final ApplicationContext context = ContextManager.getContext(deployment.getDeployedFile().getName());

        // TODO initialize core services, and join classloaders.
        // CoreClassLoaders.getInstance().register(deployment.getClassLoader());

        deployment.getQueues().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), context, c.getAnnotation(Queue.class)));
            threads.add(thread);
            executor.submit(thread);
        });

        deployment.getTopics().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector(), deployment.getClassLoader(), context, c.getAnnotation(Topic.class)));
            threads.add(thread);
            executor.submit(thread);
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

        // We manage the context here to avoid shutdown before the threads are stopped.
        ContextManager.stop(deployment.getDeployedFile().getName());

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
