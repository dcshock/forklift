package forklift.consumer.wrapper;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class RoleInputMessageTests {
    @Test
    public void testJsonEncodingEncodesFields() {
        final RoleInputMessage message = new RoleInputMessage("test-role",
                                                              "test-id",
                                                              "test-message",
                                                              Collections.singletonMap("forklift-retry-count", "2"),
                                                              Collections.singletonMap(Header.Priority, 6));
        final String jsonEncodedMessage = message.toString();

        assertTrue(jsonEncodedMessage.contains("\"role\":\"test-role\""),
                "JSON encoded message should contain correct role field");
        assertTrue(jsonEncodedMessage.contains("\"id\":\"test-id\""),
                "JSON encoded message should contain correct id field");
        assertTrue(jsonEncodedMessage.contains("\"msg\":\"test-message\""),
                "JSON encoded message should contain correct msg field");
        assertTrue(jsonEncodedMessage.contains("\"properties\":{\"forklift-retry-count\":\"2\"}"),
                "JSON encoded message should contain correct properties field");
        assertTrue(jsonEncodedMessage.contains("\"headers\":{\"Priority\":6}"),
                "JSON encoded message should contain correct headers field");

        final String expectedJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";

        assertEquals(expectedJson, jsonEncodedMessage);
    }

    @Test
    public void testJsonDecodingExtractsFields() {
        final String inputJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";
        final RoleInputMessage decodedMessage = RoleInputMessage.fromString(inputJson);

        assertEquals("test-role", decodedMessage.getRole());
        assertEquals("test-id", decodedMessage.getId());
        assertEquals("test-message", decodedMessage.getMsg());
        assertEquals(Collections.singletonMap("forklift-retry-count", "2"), decodedMessage.getProperties());
        assertEquals(Collections.singletonMap(Header.Priority, 6), decodedMessage.getHeaders());
    }

    @Test
    public void testJsonDecodingFailsWithInvalidJson() {
        assertThrows(IllegalArgumentException.class, () -> {
            final RoleInputMessage decodedMessage = RoleInputMessage.fromString("{)..v[");
        });
    }

    @Test
    public void testJsonDecodingFailsWithNullString() {
        assertThrows(IllegalArgumentException.class, () -> {
            final RoleInputMessage decodedMessage = RoleInputMessage.fromString(null);
        });
    }

    @Test
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
        assertEquals(testId, resultingMessage.getId());
        assertEquals(testMessage, resultingMessage.getMsg());
        assertEquals(testProperties, resultingMessage.getProperties());
        assertEquals(testHeaders, resultingMessage.getHeaders());
    }

    @Test
    public void testForkliftMessageCreationFromNullFieldsGivesDefaultValues() {
        final RoleInputMessage roleMessage = new RoleInputMessage(null,
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null);
        final ForkliftMessage resultingMessage = roleMessage.toForkliftMessage(null);

        assertNull(resultingMessage.getId());
        assertNull(resultingMessage.getMsg());
        assertEquals(Collections.emptyMap(), resultingMessage.getProperties());
        assertEquals(Collections.emptyMap(), resultingMessage.getHeaders());
    }

    @Test
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

        assertEquals(testRole, roleMessage.getRole());
        assertEquals(testId, roleMessage.getId());
        assertEquals(testMessage, roleMessage.getMsg());
        assertEquals(testProperties, roleMessage.getProperties());
        assertEquals(testHeaders, roleMessage.getHeaders());
    }

    @Test
    public void testRemappedJsonEqualsOriginalJson() {
        final String inputJson = "{" +
            "\"role\":\"test-role\"," +
            "\"id\":\"test-id\"," +
            "\"msg\":\"test-message\"," +
            "\"properties\":{\"forklift-retry-count\":\"2\"}," +
            "\"headers\":{\"Priority\":6}" +
            "}";

        assertEquals(inputJson, RoleInputMessage.fromString(inputJson).toString());
    }

    @Test
    public void testRemappedMessageEqualsOriginalMessage() {
        final RoleInputMessage message = new RoleInputMessage("test-role",
                                                              "test-id",
                                                              "test-message",
                                                              Collections.singletonMap("forklift-retry-count", "2"),
                                                              Collections.singletonMap(Header.Priority, 6));
        assertEquals(message, RoleInputMessage.fromString(message.toString()));
    }

    @Test
    public void testRemappedForkliftMessageAndEqualsOriginal() {
        final ForkliftMessage sourceMessage = new ForkliftMessage();
        sourceMessage.setId("test-id");
        sourceMessage.setMsg("test-message");
        sourceMessage.setProperties(Collections.singletonMap("forklift-retry-count", "2"));
        sourceMessage.setHeaders(Collections.singletonMap(Header.Priority, 6));

        final RoleInputMessage roleMessage = RoleInputMessage.fromForkliftMessage("test-role", sourceMessage);
        final ForkliftMessage outMessage = roleMessage.toForkliftMessage(sourceMessage);

        assertEquals(sourceMessage.getId(), outMessage.getId());
        assertEquals(sourceMessage.getMsg(), outMessage.getMsg());
        assertEquals(sourceMessage.getProperties(), outMessage.getProperties());
        assertEquals(sourceMessage.getHeaders(), outMessage.getHeaders());
    }

    @Test
    public void testAcknowledgmentWIthNullSourceMessage() throws ConnectorException {
        final String inputJson = "{\"msg\":\"test-message\",\"headers\":{},\"properties\":{}}";
        final RoleInputMessage roleMessage = RoleInputMessage.fromString(inputJson);
        final ForkliftMessage resultMessage = roleMessage.toForkliftMessage(null);

        assertTrue(resultMessage.acknowledge());
    }

    @Test
    public void testAcknowledgmentMapsToSourceMessage() throws ConnectorException {
        final String inputJson = "{\"msg\":\"test-message\",\"headers\":{},\"properties\":{}}";

        final TestAcknowledgmentMessage ackMessage = new TestAcknowledgmentMessage();
        ackMessage.setMsg(inputJson);
        assertFalse(ackMessage.acknowledged);

        final RoleInputMessage roleMessage = RoleInputMessage.fromString(inputJson);
        final ForkliftMessage resultMessage = roleMessage.toForkliftMessage(ackMessage);

        resultMessage.acknowledge();
        assertTrue(ackMessage.acknowledged);
    }

    @Test
    public void testMessagesWithIdenticalFieldsAreEqual() {
        final String testRole = "test-role";
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);

        assertEquals(new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders),
                new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders));
    }

    @Test
    public void testMessagesWithDifferentFieldsAreNotEqual() {
        final String testRole = "test-role";
        final String testId = "test-id";
        final String testMessage = "test-message";
        final Map<String, String> testProperties = Collections.singletonMap("forklift-retry-count", "2");
        final Map<Header, Object> testHeaders = Collections.singletonMap(Header.Priority, 6);

        assertNotEquals(new RoleInputMessage(testRole, testId, testMessage, testProperties, testHeaders),
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
