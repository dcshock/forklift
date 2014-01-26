package forklift.consumer;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.google.common.io.Files;

import forklift.ForkliftTest;
import forklift.deployment.DeploymentWatch;

public class ConsumerDeploymentEventsTest {
    @Test
    public void deploy() 
      throws IOException {
        final File deployDir = Files.createTempDir();
        final DeploymentWatch watch = new DeploymentWatch(deployDir, new ConsumerDeploymentEvents());
      
        File deployFile = File.createTempFile("test", ".jar", deployDir);
        deployFile.deleteOnExit();
        
        Files.copy(ForkliftTest.testJar(), deployFile);
        watch.run();
        
        deployFile.delete();
        watch.run();
    }
}
