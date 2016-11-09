package forklift.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import forklift.ForkliftTest;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeploymentWatchTest {
    @Test
    public void watch()
              throws IOException {
        final File dir = Files.createTempDir();
        try {
            // Create a new deployment jar.
            final File file = File.createTempFile("test", ".jar", dir);
            Files.copy(ForkliftTest.testJar(), file);

            FileDeployment fileDeployment = new FileDeployment();
            fileDeployment.setDeployedFile(file);

            final AtomicBoolean deploy = new AtomicBoolean(true);
            DeploymentWatch watch = new DeploymentWatch(dir, new DeploymentEvents() {
                @Override
                public void onDeploy(Deployment deployment) {
                    assertTrue(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }

                @Override
                public void onUndeploy(Deployment deployment) {
                    assertFalse(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }
            });

            watch.run();
            deploy.set(false);
            file.delete();
            watch.run();
        } finally {
            dir.delete();
        }
    }

    @Test
    public void loadProperties()
      throws IOException {
        final File dir = Files.createTempDir();
        try {
            // Create a new properties files.
            final File file = File.createTempFile("test", ".properties", dir);
            FileOutputStream fos = new FileOutputStream(file);
            final String props = "db=prod\ndeploy=prod\nusername=mysql\npassword=mysql\n";
            fos.write(props.getBytes());
            fos.close();
            FileDeployment fileDeployment = new FileDeployment();
            fileDeployment.setDeployedFile(file);

            final AtomicBoolean deploy = new AtomicBoolean(true);
            DeploymentWatch watch = new DeploymentWatch(dir, new DeploymentEvents() {
                @Override
                public void onDeploy(Deployment deployment) {
                    assertTrue(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }

                @Override
                public void onUndeploy(Deployment deployment) {
                    assertFalse(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }
            });

            watch.run();
            deploy.set(false);
            file.delete();
            watch.run();
        } finally {
            dir.delete();
        }
    }

    @Test
    public void propsWithComment()
      throws IOException {
        final File dir = Files.createTempDir();
        try {
            // Create a new properties files.
            final File file = File.createTempFile("test", ".properties", dir);
            FileOutputStream fos = new FileOutputStream(file);
            final String props = "db=prod\n#Db Creds\n\ndb.username=mysql\ndb.password=mysql\n";
            fos.write(props.getBytes());
            fos.close();
            FileDeployment fileDeployment = new FileDeployment();
            fileDeployment.setDeployedFile(file);


            final AtomicBoolean deploy = new AtomicBoolean(true);
            DeploymentWatch watch = new DeploymentWatch(dir, new DeploymentEvents() {
                @Override
                public void onDeploy(Deployment deployment) {
                    assertTrue(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }

                @Override
                public void onUndeploy(Deployment deployment) {
                    assertFalse(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }
            });

            watch.run();
            deploy.set(false);
            file.delete();
            watch.run();
        } finally {
            dir.delete();
        }
    }

    @Test
    public void badProps()
      throws IOException {
        final File dir = Files.createTempDir();
        try {
            // Create a new properties files.
            final File file = File.createTempFile("test", ".properties", dir);
            FileOutputStream fos = new FileOutputStream(file);
            // Create a bad props file to make sure we handle them.
            final String props = "db:prod\n\\u00sx=blah\nusername mysql\npassword=mysql\n";
            fos.write(props.getBytes());
            fos.close();
            FileDeployment fileDeployment = new FileDeployment();
            fileDeployment.setDeployedFile(file);

            final AtomicBoolean deploy = new AtomicBoolean(true);
            DeploymentWatch watch = new DeploymentWatch(dir, new DeploymentEvents() {
                @Override
                public void onDeploy(Deployment deployment) {
                    assertTrue(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }

                @Override
                public void onUndeploy(Deployment deployment) {
                    assertFalse(deploy.get());
                    assertEquals(fileDeployment, deployment);
                }
            });

            watch.run();
            deploy.set(false);
            file.delete();
            watch.run();
        } finally {
            dir.delete();
        }
    }
}
