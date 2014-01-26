package forklift.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;

public class ConsumerDeploymentEvents implements DeploymentEvents {
    private Logger log = LoggerFactory.getLogger(ConsumerDeploymentEvents.class);
    
    @Override
    public void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);
    }

    @Override
    public void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);
    }
}
