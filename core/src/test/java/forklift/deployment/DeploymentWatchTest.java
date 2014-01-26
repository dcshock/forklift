package forklift.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.google.common.io.Files;

import forklift.ForkliftTest;

public class DeploymentWatchTest {
    @Test
    public void watch() 
      throws IOException {
        final File dir = Files.createTempDir();
        
        // Create a new deployment jar.
        final File file = File.createTempFile("test", ".jar", dir);
        Files.copy(ForkliftTest.testJar(), file);
        
        final AtomicBoolean deploy = new AtomicBoolean(true);
        DeploymentWatch watch = new DeploymentWatch(dir, new DeploymentEvents() {
            @Override
            public void onDeploy(Deployment deployment) {
                assertTrue(deploy.get());
                assertEquals(file, deployment.getDeployedFile());
            }

            @Override
            public void onUndeploy(Deployment deployment) {
                assertFalse(deploy.get());
                assertEquals(file, deployment.getDeployedFile());
            }
        });
        
        watch.run();
        deploy.set(false);
        file.delete();
        watch.run();
    }
}
