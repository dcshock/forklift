package forklift.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import forklift.ForkliftTest;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class FileDeploymentTest {
    @Test(expected = IOException.class)
    public void testNullDeployment() throws IOException {
        new FileDeployment(null);
    }

    // Any kind of bad Jar file should only throw IOExcpetion otherwise we may bring down the system
    @Test(expected = IOException.class)
    public void testEmptyDeployment() throws IOException {
        File f = File.createTempFile("test", ".txt");

        try {
            new FileDeployment(f);
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
            assertEquals(0, d.getQueues().size());
            assertEquals(0, d.getTopics().size());
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
        assertEquals(2, d.getQueues().size());
        assertEquals(2, d.getTopics().size());
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
        assertEquals(4, d.getQueues().size());
        assertEquals(2, d.getTopics().size());
        // Make sure we don't accidentally delete on deploy
        assertTrue(f.exists());
    }
}
