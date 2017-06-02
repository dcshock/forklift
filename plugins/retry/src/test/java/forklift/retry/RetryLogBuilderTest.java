package forklift.retry;

import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.ForkliftSerializer;
import forklift.consumer.Consumer;
import forklift.consumer.ProcessStep;
import forklift.source.ActionSource;
import forklift.source.SourceI;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RetryLogBuilderTest {
    private static final int maxRetries = 5;
    private static final Retry retry = new Retry() {
        @Override
        public String role() {
            return "retry-role";
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public long timeout() {
            return 10;
        }

        public int maxRetries() {
            return maxRetries;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
            return Retry.class;
        }
    };

    private static final Retry noRoleRetry = new Retry() {
        @Override
        public String role() {
            return "";
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public long timeout() {
            return 10;
        }

        public int maxRetries() {
            return maxRetries;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
            return Retry.class;
        }
    };

    @Test
    public void testFieldsSetCorrectlyWithoutSerializer() {
        // build the message
        final String testId = "test-id";
        final String testText = "test-text";

        final ForkliftMessage msg = new ForkliftMessage();
        msg.setId(testId);
        msg.setMsg(testText);
        msg.setProperties(new HashMap() {{
            put("random-property", "5");
            put("random-property-2", "hi");
        }});

        final String testError = "ERROR OCCURRED";
        final List<String> errors = Arrays.asList(testError);

        // build the consumer
        final String testRole = "test-role";
        final String testSourceName = "test-queue";
        final SourceI testSource = new QueueSource(testSourceName);
        final RoleInputSource roleSource = new RoleInputSource(testRole);

        final Consumer consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.getSource()).thenReturn(testSource);
        Mockito.doReturn(TestHandler.class).when(consumer).getMsgHandler();
        Mockito.when(consumer.getRoleSources(RoleInputSource.class)).thenReturn(Stream.of(roleSource));

        //build the fake connector
        final String destinationName = "test-destination";
        final ActionSource actionSource = new QueueSource(destinationName);

        final ForkliftConnectorI connector = Mockito.mock(ForkliftConnectorI.class);
        Mockito.when(connector.mapSource(roleSource)).thenReturn(actionSource);

        // run the log builder
        final RetryLogBuilder logBuilder = new RetryLogBuilder(msg, consumer, errors, connector, retry);
        final Map<String, String> fields = logBuilder.getFields();

        Assert.assertEquals(testRole, fields.get("role"));
        Assert.assertEquals("Error", fields.get("step"));

        Assert.assertEquals("queue", fields.get("destination-type"));
        Assert.assertEquals(destinationName, fields.get("destination-name"));
        Assert.assertEquals("raw-string", fields.get("destination-message-format"));
        Assert.assertEquals(testText, fields.get("text"));

        Assert.assertEquals("5", fields.get("random-property"));

        Assert.assertEquals("1", fields.get("forklift-retry-count"));
        Assert.assertEquals("" + maxRetries, fields.get("forklift-retry-max-retries"));

        Assert.assertNotNull(fields.get("forklift-retry-version"));
        Assert.assertNotNull(fields.get("source-description"));
        Assert.assertNotNull(fields.get("destination-connector"));
        Assert.assertNotNull(fields.get("time"));
    }

    @Test
    public void testSerializerFieldsSetCorrectly() {
        // build the message
        final String testId = "test-id";
        final String testText = "test-text";

        final ForkliftMessage msg = new ForkliftMessage();
        msg.setId(testId);
        msg.setMsg(testText);

        final String testError = "ERROR OCCURRED";
        final List<String> errors = Arrays.asList(testError);

        // build the consumer
        final String testRole = "test-role";
        final String testSourceName = "test-queue";
        final SourceI testSource = new QueueSource(testSourceName);
        final RoleInputSource roleSource = new RoleInputSource(testRole);

        final Consumer consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.getSource()).thenReturn(testSource);
        Mockito.doReturn(TestHandler.class).when(consumer).getMsgHandler();
        Mockito.when(consumer.getRoleSources(RoleInputSource.class)).thenReturn(Stream.of(roleSource));

        //build the fake connector
        final String destinationName = "test-destination";
        final ActionSource actionSource = new QueueSource(destinationName);
        final byte[] testBytes = new byte[]{'h', 'i'};
        final String encodedBytes = Base64.getEncoder().encodeToString(testBytes);


        final ForkliftSerializer serializer = Mockito.mock(ForkliftSerializer.class);
        Mockito.when(serializer.serializeForSource(Mockito.eq(roleSource), Mockito.any()))
            .thenReturn(testBytes);

        final ForkliftConnectorI connector = Mockito.mock(ForkliftConnectorI.class);
        Mockito.when(connector.mapSource(roleSource)).thenReturn(actionSource);
        Mockito.when(connector.getDefaultSerializer()).thenReturn(serializer);

        // run the log builder
        final RetryLogBuilder logBuilder = new RetryLogBuilder(msg, consumer, errors, connector, retry);
        final Map<String, String> fields = logBuilder.getFields();

        Assert.assertEquals("Error", fields.get("step"));
        Assert.assertEquals(testError, fields.get("errors"));
        Assert.assertEquals(testRole, fields.get("role"));

        Assert.assertEquals("queue", fields.get("destination-type"));
        Assert.assertEquals(destinationName, fields.get("destination-name"));
        Assert.assertEquals("base64-bytes", fields.get("destination-message-format"));
        Assert.assertEquals(encodedBytes, fields.get("destination-message"));
        Assert.assertEquals(testText, fields.get("text"));

        Assert.assertNotNull(fields.get("forklift-retry-version"));
        Assert.assertNotNull(fields.get("source-description"));
        Assert.assertNotNull(fields.get("destination-connector"));
        Assert.assertNotNull(fields.get("time"));
    }

    @Test
    public void testFallbackRole() {
        // build the message
        final String testId = "test-id";
        final String testText = "test-text";

        final ForkliftMessage msg = new ForkliftMessage();
        msg.setId(testId);
        msg.setMsg(testText);

        final List<String> errors = Arrays.asList();

        // build the consumer
        final String testRole = ""; //empty role
        final String testSourceName = "test-queue";
        final SourceI testSource = new QueueSource(testSourceName);
        final RoleInputSource roleSource = new RoleInputSource(testRole);

        final Consumer consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.getSource()).thenReturn(testSource);
        Mockito.doReturn(TestHandler.class).when(consumer).getMsgHandler();
        Mockito.when(consumer.getRoleSources(RoleInputSource.class))
            .thenAnswer(invocation -> Stream.of(roleSource));

        //build the fake connector
        final String destinationName = "test-destination";
        final ActionSource actionSource = new QueueSource(destinationName);

        final ForkliftConnectorI connector = Mockito.mock(ForkliftConnectorI.class);
        Mockito.when(connector.mapSource(roleSource)).thenReturn(actionSource);

        // run the log builder
        RetryLogBuilder logBuilder = new RetryLogBuilder(msg, consumer, errors, connector, retry);
        Map<String, String> fields = logBuilder.getFields();
        Assert.assertEquals("retry-role", fields.get("role"));

        logBuilder = new RetryLogBuilder(msg, consumer, errors, connector, noRoleRetry);
        fields = logBuilder.getFields();
        Assert.assertEquals("TestHandler", fields.get("role"));
    }

    @Test
    public void testRetriesExceeded() {
        // build the message
        final String testId = "test-id";
        final String testText = "test-text";

        final ForkliftMessage msg = new ForkliftMessage();
        msg.setId(testId);
        msg.setMsg(testText);
        msg.setProperties(new HashMap() {{
            put("forklift-retry-count", "10");
            put("forklift-retry-max-retries", "10");
        }});

        final List<String> errors = Arrays.asList();

        // build the consumer
        final String testRole = ""; //empty role
        final String testSourceName = "test-queue";
        final SourceI testSource = new QueueSource(testSourceName);
        final RoleInputSource roleSource = new RoleInputSource(testRole);

        final Consumer consumer = Mockito.mock(Consumer.class);
        Mockito.when(consumer.getSource()).thenReturn(testSource);
        Mockito.doReturn(TestHandler.class).when(consumer).getMsgHandler();
        Mockito.when(consumer.getRoleSources(RoleInputSource.class))
            .thenAnswer(invocation -> Stream.of(roleSource));

        //build the fake connector
        final String destinationName = "test-destination";
        final ActionSource actionSource = new QueueSource(destinationName);

        final ForkliftConnectorI connector = Mockito.mock(ForkliftConnectorI.class);
        Mockito.when(connector.mapSource(roleSource)).thenReturn(actionSource);

        // run the log builder
        final RetryLogBuilder logBuilder = new RetryLogBuilder(msg, consumer, errors, connector, retry);
        final Map<String, String> fields = logBuilder.getFields();
        Assert.assertNull(fields);

        Assert.assertEquals("true", msg.getProperties().get("forklift-retry-max-retries-exceeded"));
    }

    private static class TestHandler {}
}
