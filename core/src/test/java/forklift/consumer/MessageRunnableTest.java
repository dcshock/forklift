package forklift.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.LifeCycle;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.source.decorators.Queue;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Queue("X")
public class MessageRunnableTest {
    private static Logger log = LoggerFactory.getLogger(MessageRunnableTest.class);

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

    /**
     * Mock consumer that can give the lifeCycle to the message runnables below.
     */
    public class BaseTestConsumer extends Consumer {
        public BaseTestConsumer(Forklift forklift) {
            super(MessageRunnableTest.class, forklift);
        }
    }

    // Given a message runner, make sure that the invalid is called when the OnValidate signature
    // is incorrect.
    @Test
    public void invalidOnValidate() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener1.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer1.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer1 tc = new TestConsumer1();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("1"), null, tc, onMessage, onValidate, null,
                                            createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer1.success.get());
        forklift.getLifeCycle().deregister(TestListener1.class);
    }

    // Supportive classes for invalidOnValidate
    public static class TestConsumer1 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            // This shouldn't run. If it does we have an issue
            TestConsumer1.success.set(false);
        }

        // The following has a bad return type for an OnValidate
        @OnValidate
        public void validateMsg() {
        }
    }

    public static class TestListener1 {
        @LifeCycle(ProcessStep.Invalid)
        public static void invalid(MessageRunnable mr) {
            TestConsumer1.success.set(true);
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            // This shouldn't run. If it does we have an issue
            TestConsumer1.success.set(false);
        }
    }

    // Given a message runner, make sure that validating is called but processing is not
    // when a message doesn't validate with a boolean return of false.
    @Test
    public void invalidMessage() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener2.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer2.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer2 tc = new TestConsumer2();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("2"), null, tc, onMessage, onValidate, null,
                                            createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer2.success.get());
        forklift.getLifeCycle().deregister(TestListener2.class);
    }

    // Supportive classes for invalidMessage
    public static class TestConsumer2 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            // This shouldn't run. If it does we have an issue
            TestConsumer2.success.set(false);
        }

        @OnValidate
        public Boolean validateMsg() {
            return false;
        }
    }

    public static class TestListener2 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer2.success.set(true);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            // This shouldn't run. If it does we have an issue
            TestConsumer2.success.set(false);
        }

        @LifeCycle(ProcessStep.Error)
        public static void error(MessageRunnable mr) {
            TestConsumer2.success.set(true);
        }
    }

    // For a message runner make sure that if boolean validation succeeds that processing is called.
    @Test
    public void validMessage() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener3.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer3.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer3 tc = new TestConsumer3();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("3"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer3.success.get());
        forklift.getLifeCycle().deregister(TestListener3.class);
    }

    // Supportive classes for validMessage
    public static class TestConsumer3 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            log.debug("processing");
        }

        @OnValidate
        public boolean validateMsg() {
            return true;
        }
    }

    public static class TestListener3 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer3.success.set(false);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            log.debug("processing");
            TestConsumer3.success.set(true);
        }

        @LifeCycle(ProcessStep.Error)
        public static void error(MessageRunnable mr) {
            TestConsumer3.success.set(false);
        }
    }

    // For a message runner make sure that if an empty List of errors in validation does call processing and completes.
    @Test
    public void emptyListRet() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener4.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer4.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer4 tc = new TestConsumer4();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("4"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer4.success.get());
        forklift.getLifeCycle().deregister(TestListener4.class);
    }

    // Supportive classes for emptyListRet
    public static class TestConsumer4 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            log.debug("processing");
        }

        @OnValidate
        public List<String> validateMsg() {
            // Send back empty list
            TestConsumer4.success.set(false);
            return new ArrayList<String>();
        }
    }

    public static class TestListener4 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer4.success.set(false);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            log.debug("processing");
        }

        @LifeCycle(ProcessStep.Error)
        public static void error(MessageRunnable mr) {
            TestConsumer4.success.set(false);
        }

        @LifeCycle(ProcessStep.Complete)
        public static void complete(MessageRunnable mr) {
            TestConsumer4.success.set(true);
        }
    }

    // For a message runner make sure that if the List of errors contains a string that it does not process and errors out.
    @Test
    public void errorInRetList() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener5.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer5.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer5 tc = new TestConsumer5();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("5"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer5.success.get());
        forklift.getLifeCycle().deregister(TestListener5.class);
    }

    // Supportive classes for errorInRetList
    public static class TestConsumer5 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            TestConsumer5.success.set(false);
            log.debug("processing");
        }

        @OnValidate
        public List<String> validateMsg() {
            List<String> list = new ArrayList<>();
            list.add("boogers");
            return list;
        }
    }

    public static class TestListener5 {
        @LifeCycle(ProcessStep.Validating)
        public static void validate(MessageRunnable mr) {
            TestConsumer5.success.set(false);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            TestConsumer5.success.set(false);
            log.debug("processing");
        }

        @LifeCycle(ProcessStep.Invalid)
        public static void invalid(MessageRunnable mr) {
            TestConsumer5.success.set(true);
        }
    }

    // For a message runner make sure that if null is returned for the list of errors from validate
    // that it calls processing.
    @Test
    public void invalidListNull() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener6.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer6.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer6 tc = new TestConsumer6();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("6"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer6.success.get());
        forklift.getLifeCycle().deregister(TestListener6.class);
    }

    // Supportive classes for invalidListNull
    public static class TestConsumer6 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            TestConsumer6.success.set(true);
            log.debug("processing");
        }

        @OnValidate
        public List<String> validateMsg() {
            return null;
        }
    }

    public static class TestListener6 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer6.success.set(false);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            log.debug("processing");
            TestConsumer6.success.set(true);
        }

        @LifeCycle(ProcessStep.Error)
        public static void error(MessageRunnable mr) {
            TestConsumer6.success.set(false);
        }
    }

    // What happens when validation throws an exception? Should throw error with reason of Exception.
    @Test
    public void validationException() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener7.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer7.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer7 tc = new TestConsumer7();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("7"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer7.success.get());
        forklift.getLifeCycle().deregister(TestListener7.class);
    }

    // Supportive classes for validationException
    public static class TestConsumer7 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            TestConsumer7.success.set(false);
            log.debug("processing");
        }

        @OnValidate
        public List<String> validateMsg() {
            throw new RuntimeException("Error Validating");
        }
    }

    public static class TestListener7 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer7.success.set(false);
            log.debug("checing for known error: " + mr.getErrors());
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            log.debug("processing");
            TestConsumer7.success.set(false);
        }

        @LifeCycle(ProcessStep.Invalid)
        public static void error(MessageRunnable mr) {
            log.debug(mr.getErrors().toString());
            if (mr.getErrors().toString().contains("Error Validating"))
                TestConsumer7.success.set(true);
        }
    }

    // What happens when processing throws an exception? Should process with Error.
    @Test
    public void processException() {
        final BaseTestConsumer consumer = new BaseTestConsumer(forklift);

        forklift.getLifeCycle().register(TestListener8.class);

        List<Method> onMessage = new ArrayList<>();
        List<Method> onValidate = new ArrayList<>();

        for (Method m : TestConsumer8.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(OnMessage.class))
                onMessage.add(m);
            else if (m.isAnnotationPresent(OnValidate.class))
                onValidate.add(m);
        }

        TestConsumer8 tc = new TestConsumer8();

        MessageRunnable
                        mr =
                        new MessageRunnable(consumer, new ForkliftMessage("8"), tc.getClass().getClassLoader(), tc, onMessage,
                                            onValidate, null, createOnProcessStepMap(), Collections.emptyList());
        mr.run();
        assertTrue(TestConsumer8.success.get());
        forklift.getLifeCycle().deregister(TestListener8.class);
    }

    // Supportive classes for processException
    public static class TestConsumer8 {
        public static AtomicBoolean success = new AtomicBoolean(false);

        @OnMessage
        public void consumeMsg() {
            TestConsumer8.success.set(false);
            throw new RuntimeException("Error processing");
        }

        @OnValidate
        public boolean validateMsg() {
            return true;
        }
    }

    public static class TestListener8 {
        @LifeCycle(ProcessStep.Validating)
        public static void invalid(MessageRunnable mr) {
            TestConsumer8.success.set(false);
        }

        @LifeCycle(ProcessStep.Processing)
        public static void process(MessageRunnable mr) {
            log.debug("processing");
            TestConsumer8.success.set(false);
        }

        @LifeCycle(ProcessStep.Error)
        public static void error(MessageRunnable mr) {
            log.debug(mr.getErrors().toString());
            if (mr.getErrors().toString().contains("Error processing"))
                TestConsumer8.success.set(true);
        }
    }

    // Creates a mock map with empty on-trigger lists
    private Map<ProcessStep, List<Method>> createOnProcessStepMap() {
        Map<ProcessStep, List<Method>> map = new HashMap<>();
        Arrays.stream(ProcessStep.values()).forEach(step -> map.put(step, new ArrayList<>()));
        return map;
    }
}
