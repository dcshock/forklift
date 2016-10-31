package forklift.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import forklift.ForkliftTest;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class DeploymentManagerTest {
    @Test
    public void deployJar()
      throws IOException {
        DeploymentManager deploymentManager = new DeploymentManager();

        final File jar = ForkliftTest.testJar();
        FileDeployment deployment = deploymentManager.registerDeployedFile(jar);
        assertTrue(deploymentManager.isDeployed(jar));
        assertEquals(1, deployment.getQueues().size());

        for (Class<?> clazz : deployment.getQueues())
            assertEquals("forklift.consumer.TestQueueConsumer", clazz.getName());

        deploymentManager.unregisterDeployedFile(jar);
        assertFalse(deploymentManager.isDeployed(jar));
    }
}
