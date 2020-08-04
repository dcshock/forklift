package forklift.deployment;

import forklift.ForkliftTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class DeploymentManagerTest {
    @Test
    public void deployJar()
                    throws IOException {
        DeploymentManager deploymentManager = new DeploymentManager();
        final File jar = ForkliftTest.testJar();
        FileDeployment deployment = deploymentManager.registerDeployedFile(jar);
        assertTrue(deploymentManager.isDeployed(jar));
        assertEquals(1, deployment.getConsumers().size());

        for (Class<?> clazz : deployment.getConsumers())
            assertEquals("forklift.consumer.TestQueueConsumer", clazz.getName());

        deploymentManager.unregisterDeployedFile(jar);
        assertFalse(deploymentManager.isDeployed(jar));
    }
}
