package forklift.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.MalformedURLException;

import org.junit.Test;

import forklift.ForkliftTest;

public class DeploymentManagerTest extends ForkliftTest {
    @Test
    public void deployJar() 
      throws MalformedURLException {
        DeploymentManager deploymentManager = 
            forklift.getContext().getBean(DeploymentManager.class);

        final File jar = new File("src/test/resources/forklift-test-consumer-0.1.jar");
        Deployment deployment = deploymentManager.registerDeployedFile(jar);
        assertTrue(deploymentManager.isDeployed(jar));
        assertEquals(1, deployment.getQueues().size());
        
        for (Class<?> clazz : deployment.getQueues()) 
            assertEquals("forklift.consumer.TestQueueConsumer", clazz.getName());
        
        deploymentManager.unregisterDeployedFile(jar);
        assertFalse(deploymentManager.isDeployed(jar));
    }
}
