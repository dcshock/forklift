

package forklift.consumer;

import org.junit.Assert;

import forklift.TestMsg;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ProcessStep;
import forklift.consumer.MessageRunnable;
import forklift.decorators.On;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Message;

public class OnDecoratorTest {

    @Test
    public void onProcessStepHappyPath() {
        Message jmsMsg = new TestMsg("Happy");

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();
        Map<ProcessStep, List<Method>> onProcessStep = new HashMap<>();

        for (Method m : TestConsumerHappy.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(On.class)) {
                onProcessStep.compute(m.getAnnotation(On.class).value(), (step, tasks) -> {
                    if (tasks == null)
                        tasks = new ArrayList<>();
                    tasks.add(m);
                    return tasks;
                });
            }
        }

        TestConsumerHappy tc = new TestConsumerHappy();

        MessageRunnable mr = new MessageRunnable(null, new ForkliftMessage(jmsMsg), tc.getClass().getClassLoader(), tc, onMessage, onValidate, onProcessStep);
        mr.run();
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Processing, ProcessStep.Complete};
        Assert.assertArrayEquals(expected, tc.path.toArray());
    }

    // Test class for testing @On annotation
    // Different input messages result in different results
    public class TestConsumerHappy {
        public List<ProcessStep> path = new ArrayList<>();

        @OnValidate
        public boolean validation() {
            return true;
        }

        @OnMessage
        public void processing() {
            // success
        }

        @On(ProcessStep.Validating)
        public void v() {
            path.add(ProcessStep.Validating);
        }

        @On(ProcessStep.Processing)
        public void p() {
            path.add(ProcessStep.Processing);
        }

        @On(ProcessStep.Invalid)
        public void i() {
            path.add(ProcessStep.Invalid);
        }

        @On(ProcessStep.Error)
        public void e() {
            path.add(ProcessStep.Error);
        }

        @On(ProcessStep.Complete)
        public void c() {
            path.add(ProcessStep.Complete);
        }
    }

    @Test
    public void onProcessStepInvalidPath() {
        Message jmsMsg = new TestMsg("Invalid");

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();
        Map<ProcessStep, List<Method>> onProcessStep = new HashMap<>();

        for (Method m : TestConsumerInvalid.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(On.class)) {
                onProcessStep.compute(m.getAnnotation(On.class).value(), (step, tasks) -> {
                    if (tasks == null)
                        tasks = new ArrayList<>();
                    tasks.add(m);
                    return tasks;
                });
            }
        }

        TestConsumerInvalid tc = new TestConsumerInvalid();

        MessageRunnable mr = new MessageRunnable(null, new ForkliftMessage(jmsMsg), tc.getClass().getClassLoader(), tc, onMessage, onValidate, onProcessStep);
        mr.run();
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Invalid};
        Assert.assertArrayEquals(expected, tc.path.toArray());
    }

    // Test class for testing @On annotation
    // Different input messages result in different results
    public class TestConsumerInvalid {
        public List<ProcessStep> path = new ArrayList<>();

        @OnValidate
        public boolean validation() {
            System.out.println("ever here??");
            return false;
        }

        @OnMessage
        public void processing() {
            // should never get here
        }

        @On(ProcessStep.Validating)
        public void v() {
            path.add(ProcessStep.Validating);
        }

        @On(ProcessStep.Processing)
        public void p() {
            path.add(ProcessStep.Processing);
        }

        @On(ProcessStep.Invalid)
        public void i() {
            path.add(ProcessStep.Invalid);
        }

        @On(ProcessStep.Error)
        public void e() {
            path.add(ProcessStep.Error);
        }

        @On(ProcessStep.Complete)
        public void c() {
            path.add(ProcessStep.Complete);
        }
    }

    @Test
    public void onProcessStepErrorPath() {
        Message jmsMsg = new TestMsg("Error");

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();
        Map<ProcessStep, List<Method>> onProcessStep = new HashMap<>();

        for (Method m : TestConsumerError.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
            else if (m.isAnnotationPresent(On.class)) {
                onProcessStep.compute(m.getAnnotation(On.class).value(), (step, tasks) -> {
                    if (tasks == null)
                        tasks = new ArrayList<>();
                    tasks.add(m);
                    return tasks;
                });
            }
        }

        TestConsumerError tc = new TestConsumerError();

        MessageRunnable mr = new MessageRunnable(null, new ForkliftMessage(jmsMsg), tc.getClass().getClassLoader(), tc, onMessage, onValidate, onProcessStep);
        mr.run();
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Processing, ProcessStep.Error};
        Assert.assertArrayEquals(expected, tc.path.toArray());
    }

    // Test class for testing @On annotation
    // Different input messages result in different results
    public class TestConsumerError {
        public List<ProcessStep> path = new ArrayList<>();

        @OnValidate
        public boolean validation() {
            return true;
        }

        @OnMessage
        public void processing() {
            throw new RuntimeException("failure");
        }

        @On(ProcessStep.Validating)
        public void v() {
            path.add(ProcessStep.Validating);
        }

        @On(ProcessStep.Processing)
        public void p() {
            path.add(ProcessStep.Processing);
        }

        @On(ProcessStep.Invalid)
        public void i() {
            path.add(ProcessStep.Invalid);
        }

        @On(ProcessStep.Error)
        public void e() {
            path.add(ProcessStep.Error);
        }

        @On(ProcessStep.Complete)
        public void c() {
            path.add(ProcessStep.Complete);
        }
    }
}
