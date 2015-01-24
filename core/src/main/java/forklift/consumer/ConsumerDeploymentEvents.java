package forklift.consumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.Forklift;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);
    
    private Map<Deployment, Integer> deployments;
    private Forklift forklift;
    private ConsumerManager manager;
    
    public ConsumerDeploymentEvents(Forklift forklift) {
    	this.deployments = new HashMap<>();
    	this.forklift = forklift;
    	this.manager = new ConsumerManager();
	}

    @Override
    public synchronized void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);
        final Set<Class<?>> s = new HashSet<Class<?>>();
        s.addAll(deployment.getQueues());
        s.addAll(deployment.getTopics());

        final Consumer c = new Consumer(forklift.getConnector(), s);
        deployments.put(deployment, manager.register(c));
    }

    @Override
    public synchronized void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);
        manager.unregister(deployments.remove(deployment));
    }
}
