package forklift.consumer;

import java.util.ArrayList;
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
    
    private Map<Deployment, List<ConsumerThread>> deployments;
    private Forklift forklift;
    
    public ConsumerDeploymentEvents(Forklift forklift) {
    	this.deployments = new HashMap<>();
    	this.forklift = forklift;
	}

    @Override
    public synchronized void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);

        final List<ConsumerThread> consumerThreads = new ArrayList<>();
        deployment.getQueues().forEach(c -> {
        	final ConsumerThread thread = new ConsumerThread(
    			new Consumer(c, forklift.getConnector()));
        	consumerThreads.add(thread);
        });
        
        deployment.getTopics().forEach(c -> {
        	final ConsumerThread thread = new ConsumerThread(
    			new Consumer(c, forklift.getConnector()));
        	consumerThreads.add(thread);
        });
        
        deployments.put(deployment, consumerThreads);
    }

    @Override
    public synchronized void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);
        final List<ConsumerThread> consumerThreads = deployments.remove(deployment);
        if (consumerThreads == null) 
        	return;
        
        consumerThreads.forEach(c -> {
        	c.getConsumer().shutdown();
        	try {
				c.join(60000);
			} catch (Exception e) {
			}
        });
    }
}
