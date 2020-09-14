package forklift.deployment;

import forklift.ForkliftTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.jupiter.api.Test;

public class FileDeploymentTest {
    @Test
    public void testNullDeployment() throws IOException {
        assertThrows(IOException.class, () -> {
            new FileDeployment(null);
        });
    }

    // Any kind of bad Jar file should only throw IOExcpetion otherwise we may bring down the system
    @Test
    public void testEmptyDeployment() throws IOException {
        File f = File.createTempFile("test", ".txt");

        try {
            assertThrows(IOException.class, () -> {
                new FileDeployment(f);
            });
        } finally {
            // Don't leave test files around
            f.delete();
        }
    }

    @Test
    public void testManifestOnlyDeploy() throws IOException {
        File f = File.createTempFile("test", ".jar");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream target = new JarOutputStream(new FileOutputStream(f.getAbsolutePath()), manifest);
        target.close();

        try {
            FileDeployment d = new FileDeployment(f);
            assertEquals(f.getName(), d.getDeployedFile().getName());
            assertTrue(d.isJar());
            assertEquals(0, d.getConsumers().size());
            // Make sure we don't accidentally delete on deploy
            assertTrue(f.exists());
        } finally {
            f.delete();
        }
    }

    // Now using a prebuilt test jar -- found in the forklift-test-consumer
    // make sure that we can deploy a jar that contains multiple classes that have some consumers
    // that consume queues and some consuming topics
    @Test
    public void testDeployJar() throws IOException {
        File f = ForkliftTest.testMultiTQJar();
        FileDeployment d = new FileDeployment(f);
        assertNotNull(d);
        assertTrue(d.isJar());
        assertEquals(3, d.getConsumers().size());
        // Make sure we don't accidentally delete on deploy
        assertTrue(f.exists());
    }

    // Now using a prebuilt test jarjar -- found in the forklift-test-consumer
    // make sure that we can deploy it that contains multiple classes that have some consumers
    // that consume queues and some consuming topics
    @Test
    public void testDeployJarJar() throws IOException {
        File f = ForkliftTest.testJarJar();
        FileDeployment d = new FileDeployment(f);
        assertNotNull(d);
        assertTrue(d.isJar());
        assertEquals(5, d.getConsumers().size());
        // Make sure we don't accidentally delete on deploy
        assertTrue(f.exists());
    }
}
