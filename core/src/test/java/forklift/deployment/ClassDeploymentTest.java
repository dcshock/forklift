package forklift.deployment;

import forklift.ForkliftTest;

import forklift.deployment.deploymentClasses.TestCoreService1;
import forklift.deployment.deploymentClasses.TestQueue1;
import forklift.deployment.deploymentClasses.TestQueue2;
import forklift.deployment.deploymentClasses.TestService1;
import forklift.deployment.deploymentClasses.TestTopic1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class ClassDeploymentTest {

    @Test
    public void testNullDeployment() throws IOException {
        assertThrows(NullPointerException.class, () -> {
            new ClassDeployment((Class<?>) null);
        });
    }

    @Test
    public void testNullArrayDeployment() throws IOException {
        assertThrows(NullPointerException.class, () -> {
        new ClassDeployment((Class<?>[]) null);
        });
    }

    @Test
    public void testPartRetrieval() throws IOException {
        File f = ForkliftTest.testMultiTQJar();
        ClassDeployment d = new ClassDeployment(TestCoreService1.class, TestQueue1.class, TestQueue2.class, TestService1.class, TestTopic1.class);
        assertNotNull(d);
        assertEquals(3, d.getConsumers().size());
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
