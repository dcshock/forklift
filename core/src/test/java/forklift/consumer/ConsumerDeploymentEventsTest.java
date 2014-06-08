package forklift.consumer;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.google.common.io.Files;

import forklift.ForkliftTest;
import forklift.deployment.DeploymentWatch;
import forklift.spring.ContextManager;

public class ConsumerDeploymentEventsTest extends ForkliftTest {
    @Test
    public void deploy()
      throws IOException {
        final File deployDir = Files.createTempDir();
        final DeploymentWatch watch = new DeploymentWatch(deployDir,
            ContextManager.getContext().getBean(ConsumerDeploymentEvents.class));

        File deployFile = File.createTempFile("test", ".jar", deployDir);
        deployFile.deleteOnExit();

        Files.copy(ForkliftTest.testJar(), deployFile);
        watch.run();

        deployFile.delete();
        watch.run();
    }
}
