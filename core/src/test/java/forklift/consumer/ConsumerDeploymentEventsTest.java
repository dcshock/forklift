package forklift.consumer;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import com.google.common.io.Files;

import forklift.ForkliftTest;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentWatch;

@RunWith(JUnit4.class)
public class ConsumerDeploymentEventsTest extends ForkliftTest {
    @Test
    public void consumerDeployEvent()
      throws IOException {
        final ConsumerDeploymentEvents events = Mockito.mock(ConsumerDeploymentEvents.class);
        final File deployDir = Files.createTempDir();
        final DeploymentWatch watch = new DeploymentWatch(deployDir, events);

        File deployFile = File.createTempFile("test", ".jar", deployDir);
        deployFile.deleteOnExit();

        Files.copy(ForkliftTest.testJar(), deployFile);
        watch.run();
        Mockito.verify(events).onDeploy(Mockito.any(Deployment.class));

        deployFile.delete();
        watch.run();
        Mockito.verify(events).onUndeploy(Mockito.any(Deployment.class));
    }
}
