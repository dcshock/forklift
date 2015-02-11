package forklift.consumer;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.Forklift;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);

    private final Map<Deployment, List<Future<?>>> running;
    private final Map<Deployment, List<ConsumerThread>> deployments;
    private final Forklift forklift;
    private final ExecutorService executor;

    public ConsumerDeploymentEvents(Forklift forklift, ExecutorService executor) {
        this.running = new HashMap<>();
        this.deployments = new HashMap<>();
        this.forklift = forklift;
        this.executor = executor;
    }

    public ConsumerDeploymentEvents(Forklift forklift) {
        // A cached thread pool creates new threads as needed, but will reuse previously
        // constructed threads when they are available. Automatically closes threads after
        // 60 seconds.
        this(forklift, Executors.newCachedThreadPool());
	}

    @Override
    public synchronized void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);

        final List<ConsumerThread> threads = new ArrayList<>();
        final List<Future<?>> futures = new ArrayList<>();

        deployment.getQueues().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector()));
            threads.add(thread);
            futures.add(executor.submit(thread));
        });

        deployment.getTopics().forEach(c -> {
            final ConsumerThread thread = new ConsumerThread(
                new Consumer(c, forklift.getConnector()));
            threads.add(thread);
            futures.add(executor.submit(thread));
        });

        deployments.put(deployment, threads);
        running.put(deployment, futures);
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

        final List<Future<?>> futures = running.remove(deployment);
        if (futures != null && !futures.isEmpty()) {
            futures.forEach(f -> {
                if (!f.isCancelled()) f.cancel(true);
            });
        }
    }

}
