package forklift.deployment;

import forklift.ForkliftTest;

import forklift.deployment.deploymentClasses.TestCoreService1;
import forklift.deployment.deploymentClasses.TestQueue1;
import forklift.deployment.deploymentClasses.TestQueue2;
import forklift.deployment.deploymentClasses.TestService1;
import forklift.deployment.deploymentClasses.TestTopic1;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.Assert.*;

public class ClassDeploymentTest {

    @Test(expected = NullPointerException.class)
    public void testNullDeployment() throws IOException {
        new ClassDeployment(null);
    }

    @Test
    public void testPartRetrieval() throws IOException {
        File f = ForkliftTest.testMultiTQJar();
        ClassDeployment d = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertNotNull(d);
        assertEquals(2, d.getQueues().size());
        assertEquals(1, d.getTopics().size());
        assertEquals(1, d.getCoreServices().size());
        assertEquals(1, d.getServices().size());
    }

    @Test
    public void hashCodeEqualsTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertEquals(d1.hashCode(), d2.hashCode());
    }

    @Test
    public void hashCodeNotEqualsTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertNotEquals(d1.hashCode(), d2.hashCode());
    }

    @Test
    public void equalsReflexiveTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertTrue(d1.equals(d1));
    }
    @Test
    public void equalsSymmetricTrueTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertTrue(d1.equals(d2));
        assertTrue(d2.equals(d1));
    }
    @Test
    public void equalsSymmetricFalseTest1(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestService1.class, TestTopic1.class);
        assertFalse(d1.equals(d2));
        assertFalse(d2.equals(d1));
    }
    @Test
    public void equalsSymmetricFalseTest2(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestTopic1.class, TestTopic1.class);
        assertFalse(d1.equals(d2));
        assertFalse(d2.equals(d1));
    }
    @Test
    public void equalsTransitiveTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d2 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        ClassDeployment d3 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertTrue(d1.equals(d2));
        assertTrue(d2.equals(d3));
        assertTrue(d1.equals(d3));
    }
    @Test
    public void equalsNullReferenceTest(){
        ClassDeployment d1 = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertFalse(d1.equals(null));
    }
}
