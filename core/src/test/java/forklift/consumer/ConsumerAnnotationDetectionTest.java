package forklift.consumer;

import static org.junit.Assert.*;

import forklift.decorators.Message;
import forklift.decorators.On;
import forklift.decorators.OnMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

@RunWith(JUnit4.class)
public class ConsumerAnnotationDetectionTest {
    public static class Simple {
        @On(ProcessStep.Complete) public void complete() {}
        @On(ProcessStep.Pending) public void pending() {}
        @Message public String message;
        @OnMessage public void onMessage() {};
    }

    public static class Inherited extends Simple {
    }

    public interface Interface {
        @On(ProcessStep.Pending) void pending();
        @OnMessage void onMessage();
    }

    public static class Implementation implements Interface {
        @Override public void pending() { }
        @Override public void onMessage() { }
    }

    @Test
    public void testSimpleClass() throws Exception {
        testAnnotations(Simple.class);
    }

    @Test
    public void testInheritedClass() throws Exception {
        testAnnotations(Inherited.class);
    }

    public void testAnnotations(Class<?> classy) throws Exception {
        ConsumerAnnotationDetector detector = new ConsumerAnnotationDetector(classy, false, false);
        assertEquals(1, detector.getOnMessage().size());
        assertEquals(classy.getMethod("onMessage"), detector.getOnMessage().get(0));

        assertEquals(1, detector.getInjectFields().get(Message.class).size());
        List<Field> fields = detector.getInjectFields().get(Message.class).get(String.class);
        assertEquals(1, fields.size());
        assertEquals(classy.getField("message"), fields.get(0));

        List<Method> completeMethods = detector.getOnProcessStep().get(ProcessStep.Complete);
        assertEquals(1, completeMethods.size());
        assertEquals(classy.getMethod("complete"), completeMethods.get(0));

        List<Method> pendingMethods = detector.getOnProcessStep().get(ProcessStep.Pending);
        assertEquals(1, pendingMethods.size());
        assertEquals(classy.getMethod("pending"), pendingMethods.get(0));

        List<Method> errorMethods = detector.getOnProcessStep().get(ProcessStep.Error);
        assertEquals(0, errorMethods.size());
    }

    @Test
    public void testImplementedInterface() throws Exception {
        ConsumerAnnotationDetector detector = new ConsumerAnnotationDetector(Implementation.class, false, false);
        assertEquals(1, detector.getOnMessage().size());
        assertEquals(Interface.class.getMethod("onMessage"), detector.getOnMessage().get(0));

        List<Method> pendingMethods = detector.getOnProcessStep().get(ProcessStep.Pending);
        assertEquals(1, pendingMethods.size());
        assertEquals(Interface.class.getMethod("pending"), pendingMethods.get(0));
    }
}
