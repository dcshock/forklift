package forklift.consumer.wrapper;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class RoleInputMessageTests {
    
    public void testJsonEncodingEncodesFields() {
        final RoleInputMessage message = new RoleInputMessage("test-role",
                                                              "test-id",
                                                              "test-message",
                                                              Collections.singletonMap("forklift-retry-count", "2"),
                                                              Collections.singletonMap(Header.Priority, 6));
        final String jsonEncodedMessage = message.toString();

        Assert.assertTrue("JSON encoded message should contain correct role field",
                          jsonEncodedMessage.contains("\"role\":\"test-role\""));
        Assert.assertTrue("JSON encoded message should contain correct id field",
                          jsonEncodedMessage.contains("\"id\":\"test-id\""));
        Assert.assertTrue("JSON encoded message should contain correct msg field",
                          jsonEncodedMessage.contains("\"msg\":\"test-message\""));
        Assert.assertTrue("JSON encoded message should contain correct properties field",
                          jsonEncodedMessage.contains("\"properties\":{\"forklift-retry-count\":\"2\"}"));
        Assert.assertTrue("JSON encoded message should contain correct headers field",
                          jsonEncodedMessage.contains("\"headers\":{\"Priority\":6}"));

        final String expectedJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";

        Assert.assertEquals(expectedJson, jsonEncodedMessage);
    }

    
    public void testJsonDecodingExtractsFields() {
        final String inputJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";
        final RoleInputMessage decodedMessage = RoleInputMessage.fromString(inputJson);

        Assert.assertEquals("test-role", decodedMessage.getRole());
        Assert.assertEquals("test-id", decodedMessage.getId());
        Assert.assertEquals("test-message", decodedMessage.getMsg());
        Assert.assertEquals(Collections.singletonMap("forklift-retry-count", "2"), decodedMessage.getProperties());
        Assert.assertEquals(Collections.singletonMap(Header.Priority, 6), decodedMessage.getHeaders());
    }

    
    public void testJsonDecodingFailsWithInvalidJson() {
        final RoleInputMessage decodedMessage = RoleInputMessage.fromString("{)..v[");
    }

    
    public void testJsonDecodingFailsWithNullString() {
        final RoleInputMessage decodedMessage = RoleInputMessage.fromString(null);
    }

    
    public void testForkliftMessageCreationCopiesFields() {
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);
        final RoleInputMessage roleMessage = new RoleInputMessage("test-role",
                                                                  testId,
                                                                  testMessage,
                                                                  testProperties,
                                                                  testHeaders);

        final ForkliftMessage resultingMessage = roleMessage.toForkliftMessage(null);
        Assert.assertEquals(testId, resultingMessage.getId());
        Assert.assertEquals(testMessage, resultingMessage.getMsg());
        Assert.assertEquals(testProperties, resultingMessage.getProperties());
        Assert.assertEquals(testHeaders, resultingMessage.getHeaders());
    }

    
    public void testForkliftMessageCreationFromNullFieldsGivesDefaultValues() {
        final RoleInputMessage roleMessage = new RoleInputMessage(null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null);
        final ForkliftMessage resultingMessage = roleMessage.toForkliftMessage(null);

        Assert.assertNull(resultingMessage.getId());
        Assert.assertNull(resultingMessage.getMsg());
        Assert.assertEquals(Collections.emptyMap(), resultingMessage.getProperties());
        Assert.assertEquals(Collections.emptyMap(), resultingMessage.getHeaders());
    }

    
    public void testCreationFromForkliftMessageCopiesFields() {
        final String testRole = "test-role";
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);

        final ForkliftMessage sourceMessage = new ForkliftMessage();
        sourceMessage.setId(testId);
        sourceMessage.setMsg(testMessage);
        sourceMessage.setProperties(testProperties);
        sourceMessage.setHeaders(testHeaders);

        final RoleInputMessage roleMessage = RoleInputMessage.fromForkliftMessage(testRole, sourceMessage);

        Assert.assertEquals(testRole, roleMessage.getRole());
        Assert.assertEquals(testId, roleMessage.getId());
        Assert.assertEquals(testMessage, roleMessage.getMsg());
        Assert.assertEquals(testProperties, roleMessage.getProperties());
        Assert.assertEquals(testHeaders, roleMessage.getHeaders());
    }

    
    public void testRemappedJsonEqualsOriginalJson() {
        final String inputJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";

        Assert.assertEquals(inputJson, RoleInputMessage.fromString(inputJson).toString());
    }

    
    public void testRemappedMessageEqualsOriginalMessage() {
        final RoleInputMessage message = new RoleInputMessage("test-role",
                                                              "test-id",
                                                              "test-message",
                                                              Collections.singletonMap("forklift-retry-count", "2"),
                                                              Collections.singletonMap(Header.Priority, 6));
        Assert.assertEquals(message, RoleInputMessage.fromString(message.toString()));
    }

    
    public void testRemappedForkliftMessageAndEqualsOriginal() {
        final ForkliftMessage sourceMessage = new ForkliftMessage();
        sourceMessage.setId("test-id");
        sourceMessage.setMsg("test-message");
        sourceMessage.setProperties(Collections.singletonMap("forklift-retry-count", "2"));
        sourceMessage.setHeaders(Collections.singletonMap(Header.Priority, 6));

        final RoleInputMessage roleMessage = RoleInputMessage.fromForkliftMessage("test-role", sourceMessage);
        final ForkliftMessage outMessage = roleMessage.toForkliftMessage(sourceMessage);

        Assert.assertEquals(sourceMessage.getId(), outMessage.getId());
        Assert.assertEquals(sourceMessage.getMsg(), outMessage.getMsg());
        Assert.assertEquals(sourceMessage.getProperties(), outMessage.getProperties());
        Assert.assertEquals(sourceMessage.getHeaders(), outMessage.getHeaders());
    }

    
    public void testAcknowledgmentWIthNullSourceMessage() throws ConnectorException {
        final String inputJson = "{\"msg\":\"test-message\",\"headers\":{},\"properties\":{}}";
        final RoleInputMessage roleMessage = RoleInputMessage.fromString(inputJson);
        final ForkliftMessage resultMessage = roleMessage.toForkliftMessage(null);

        Assert.assertTrue(resultMessage.acknowledge());
    }

    
    public void testAcknowledgmentMapsToSourceMessage() throws ConnectorException {
        final String inputJson = "{\"msg\":\"test-message\",\"headers\":{},\"properties\":{}}";

        final TestAcknowledgmentMessage ackMessage = new TestAcknowledgmentMessage();
        ackMessage.setMsg(inputJson);
        Assert.assertFalse(ackMessage.acknowledged);

        final RoleInputMessage roleMessage = RoleInputMessage.fromString(inputJson);
        final ForkliftMessage resultMessage = roleMessage.toForkliftMessage(ackMessage);

        resultMessage.acknowledge();
        Assert.assertTrue(ackMessage.acknowledged);
    }

    
    public void testMessagesWithIdenticalFieldsAreEqual() {
        final String testRole = "test-role";
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);

        Assert.assertEquals(new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders),
                            new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders));
    }

    
    public void testMessagesWithDifferentFieldsAreNotEqual() {
        final String testRole = "test-role";
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);

        Assert.assertNotEquals(new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders),
                               new RoleInputMessage("other-role", testId, testMessage, testProperties, testHeaders));
    }

    private static class TestAcknowledgmentMessage extends ForkliftMessage{
        public boolean acknowledged = false;

        @Override
        public boolean acknowledge() {
            this.acknowledged = true;

            return true;
        }
    }
}
