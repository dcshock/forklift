package forklift.consumer;

import forklift.ForkliftTest;
import forklift.deployment.FileDeployment;
import forklift.deployment.DeploymentWatch;

import com.google.common.io.Files;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

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
        Mockito.verify(events).onDeploy(Mockito.any(FileDeployment.class));

        deployFile.delete();
        watch.run();
        Mockito.verify(events).onUndeploy(Mockito.any(FileDeployment.class));
    }
}
