package forklift.consumer;

import forklift.Forklift;
import forklift.concurrent.Executors;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);

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
    public synchronized void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);

        final List<ConsumerThread> threads = new ArrayList<>();

        deployment.getQueues().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector()));
            threads.add(thread);
            executor.submit(thread);
        });

        deployment.getTopics().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector()));
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
                t.getConsumer().shutdown();
                try {
                    t.join(60000);
                } catch (Exception e) {
                }
            });
        }
    }

}
