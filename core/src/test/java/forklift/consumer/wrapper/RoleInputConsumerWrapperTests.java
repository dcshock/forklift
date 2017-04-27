package forklift.consumer.wrapper;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ForkliftConsumerI;
import forklift.message.Header;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RoleInputConsumerWrapperTests {
    @Test
    public void testMessageTakenFromConsumer() throws ConnectorException {
        final String sourceJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";

        // a source message containing the message to extract;
        // only the source message should affect the result
        final ForkliftMessage sourceMessage = new ForkliftMessage();
        sourceMessage.setId("irrelevant-id");
        sourceMessage.setMsg(sourceJson);
        sourceMessage.setProperties(Collections.singletonMap("irrelevant-property", "2"));
        sourceMessage.setHeaders(Collections.singletonMap(Header.Priority, 10));

        final ForkliftConsumerI testConsumer = new ConstantMessageConsumer(sourceMessage);
        final ForkliftConsumerI consumerWrapper = new RoleInputConsumerWrapper(testConsumer);
        final ForkliftMessage extractedMessage = consumerWrapper.receive(1000);

        Assert.assertEquals("test-id", extractedMessage.getId());
        Assert.assertEquals("test-message", extractedMessage.getMsg());
        Assert.assertEquals(Collections.singletonMap("forklift-retry-count", "2"), extractedMessage.getProperties());
        Assert.assertEquals(Collections.singletonMap(Header.Priority, 6), extractedMessage.getHeaders());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidJsonMessage() throws ConnectorException {
        final String sourceJson = "{[hi";
        final ForkliftMessage sourceMessage = new ForkliftMessage(sourceJson);
        final ForkliftConsumerI testConsumer = new ConstantMessageConsumer(sourceMessage);
        final ForkliftConsumerI consumerWrapper = new RoleInputConsumerWrapper(testConsumer);

        final ForkliftMessage extractedMessage = consumerWrapper.receive(1000);
    }

    @Test
    public void testNullSourceMessage() throws ConnectorException {
        final ForkliftConsumerI testConsumer = new ConstantMessageConsumer(null);
        final ForkliftConsumerI consumerWrapper = new RoleInputConsumerWrapper(testConsumer);

        final ForkliftMessage extractedMessage = consumerWrapper.receive(1000);
        Assert.assertNull(extractedMessage);
    }

    private static class ConstantMessageConsumer implements ForkliftConsumerI {
        private ForkliftMessage outputMessage;
        public ConstantMessageConsumer(ForkliftMessage outputMessage) {
            this.outputMessage = outputMessage;
        }

        @Override
        public ForkliftMessage receive(long timeout) {
            return outputMessage;
        }

        @Override
        public void close() {}
    }
}
