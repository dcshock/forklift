package forklift.consumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;

@Component
public class ConsumerDeploymentEvents implements DeploymentEvents {
    private Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);
    private Map<Deployment, Integer> deployments = new HashMap<Deployment, Integer>();

    @Autowired
    private ConsumerManager manager;

    @Override
    public synchronized void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);
        Set<Class<?>> s = new HashSet<Class<?>>();
        s.addAll(deployment.getQueues());
        s.addAll(deployment.getTopics());

        Consumer c = new Consumer(s);
        deployments.put(deployment, manager.register(c));
    }

    @Override
    public synchronized void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);
        manager.unregister(deployments.remove(deployment));
    }
}
