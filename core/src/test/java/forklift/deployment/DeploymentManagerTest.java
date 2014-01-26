package forklift.deployment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.MalformedURLException;

import org.junit.Test;

import forklift.ForkliftTest;

public class DeploymentManagerTest extends ForkliftTest {
    @Test
    public void test() 
      throws MalformedURLException {
        DeploymentManager deployment = 
            forklift.getContext().getBean(DeploymentManager.class);

        final File jar = new File("src/test/resources/forklift-test-consumer-0.1.jar");
        deployment.registerDeployedFile(jar);
        assertTrue(deployment.isDeployed(jar));
        deployment.unregisterDeployedFile(jar);
        assertFalse(deployment.isDeployed(jar));
    }
}
