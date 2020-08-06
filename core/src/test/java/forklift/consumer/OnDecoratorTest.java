package forklift.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.On;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.source.decorators.Queue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OnDecoratorTest {

    private static Forklift forklift;
    private static ForkliftConnectorI connector;

    @BeforeAll
    public static void setup() {
        LifeCycleMonitors lifeCycle = new LifeCycleMonitors();
        forklift = mock(Forklift.class);
        connector = mock(ForkliftConnectorI.class);
        when(forklift.getLifeCycle()).thenReturn(lifeCycle);
        when(forklift.getConnector()).thenReturn(connector);
    }

    @Test
    public void onProcessStepHappyPath() {
        TestConsumerHappy tc = new TestConsumerHappy();
        runTest(tc);
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Processing, ProcessStep.Complete};
        assertArrayEquals(expected, tc.path.toArray());
    }

    @Queue("1")
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
        TestConsumerInvalid tc = new TestConsumerInvalid();
        runTest(tc);
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Invalid};
        assertArrayEquals(expected, tc.path.toArray());
    }

    @Queue("1")
    public class TestConsumerInvalid {
        public List<ProcessStep> path = new ArrayList<>();

        @OnValidate
        public boolean validation() {
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
        TestConsumerError tc = new TestConsumerError();
        runTest(tc);
        ProcessStep[] expected = {ProcessStep.Validating, ProcessStep.Processing, ProcessStep.Error};
        assertArrayEquals(expected, tc.path.toArray());
    }

    @Queue("1")
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

    @Test
    public void repeatOn() {
        TestConsumerMulti tc = new TestConsumerMulti();
        runTest(tc);
        assertEquals(3, tc.callCount);
    }

    @Queue("1")
    public class TestConsumerMulti {
        int callCount = 0;

        @OnValidate
        public boolean validation() {
            return true;
        }

        @OnMessage
        public void processing() {
            // success
        }

        @On(ProcessStep.Validating)
        @On(ProcessStep.Processing)
        @On(ProcessStep.Complete)
        @On(ProcessStep.Complete)
        public void c() {
            callCount++;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void runTest(T c) {
        final ForkliftMessage msg = new ForkliftMessage("Message");
        msg.setId("Message");
        final Consumer consumer = new Consumer(c.getClass(), forklift);
        consumer.inject(msg, c);
        List<Method> onMessage = (List<Method>) fetch(consumer, "onMessage");
        List<Method> onValidate = (List<Method>) fetch(consumer, "onValidate");
        Map<ProcessStep, List<Method>> onProcessStep = (Map<ProcessStep, List<Method>>) fetch(consumer, "onProcessStep");
        final MessageRunnable mr = new MessageRunnable(consumer, msg, consumer.getClass().getClassLoader(), c, onMessage, onValidate, null, onProcessStep, Collections.emptyList());
        mr.run();
    }

    private static Object fetch(Object object, String name) {
        try {
            final Field field = object.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(object);
        } catch (Exception e) {
            fail(e.toString());
            return null;
        }
    }
}
