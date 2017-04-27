package forklift.consumer.wrapper;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable version of {@link forklift.connectors.ForkliftMessage} for being used
 * with {@link forklift.source.decorators.RoleInput}.
 */
public class RoleInputMessage {
    private static final Logger log = LoggerFactory.getLogger(RoleInputMessage.class);
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private String role;
    private String id;
    private String msg;
    private Map<String, String> properties;
    private Map<Header, Object> headers;

    @JsonCreator
    public RoleInputMessage(@JsonProperty("role") String role,
                            @JsonProperty("id") String id,
                            @JsonProperty("msg") String msg,
                            @JsonProperty("properties") Map<String, String> properties,
                            @JsonProperty("headers") Map<Header, Object> headers) {
        this.role = role;
        this.id = id;
        this.msg = msg;
        this.properties = properties;
        this.headers = headers;
    }

    public String getRole() {
        return role;
    }

    public String getId() {
        return id;
    }

    public String getMsg() {
        return msg;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<Header, Object> getHeaders() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RoleInputMessage))
            return false;

        RoleInputMessage that = (RoleInputMessage) o;
        return Objects.equals(this.role, that.role) &&
            Objects.equals(this.id, that.id) &&
            Objects.equals(this.msg, that.msg) &&
            Objects.equals(this.properties, that.properties) &&
            Objects.equals(this.headers, that.headers);
    }

    /**
     * Decodes a message from the given JSON string.
     *
     * @param input the JSON string to read from
     * @return the mapped version of the given string
     */
    public static RoleInputMessage fromString(String input) {
        try {
            return mapper.readValue(input, RoleInputMessage.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not deserialize RoleInputMessage from JSON: " + input);
        }
    }

    /**
     * Decodes a message from a role and source {@link forklift.connectors.ForkliftMessage}.
     *
     * @param role the role for the deserialized message
     * @param input the forlift message to take info from
     * @return a role input message that contains a copy of the given forklift message
               and is assigned to the given role
     */
    public static RoleInputMessage fromForkliftMessage(String role, ForkliftMessage input) {
        return new RoleInputMessage(
            role,
            input.getId(),
            input.getMsg(),
            input.getProperties(),
            input.getHeaders());
    }

    /**
     * Encodes the message to a JSON string.
     *
     * @return this role input message, with its fields mapped to JSON
     */
    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            log.error("Could not serialize RoleInputMessage", e);
        }
        return null;
    }

    /**
     * Encodes this role input message to a forklift message, dropping the role.
     *
     * @param sourceMessage the source message to acknowledge when acknowledging the resulting message
     * @return a forklift message, initialized with the data from this role input message
     */
    public ForkliftMessage toForkliftMessage(ForkliftMessage sourceMessage) {
        final ForkliftMessage result = new AcknowledgingMessage(sourceMessage);

        if (id != null) {
            result.setId(id);
        }
        if (msg != null) {
            result.setMsg(msg);
        }
        if (properties != null) {
            result.setProperties(properties);
        }
        if (headers != null) {
            result.setHeaders(headers);
        }

        return result;
    }

    /**
     * A {@link forklift.connnectors.ForkliftMessage} that uses the given message for acknowledgment.
     */
    private static class AcknowledgingMessage extends ForkliftMessage {
        private ForkliftMessage sourceMessage;
        public AcknowledgingMessage(ForkliftMessage sourceMessage) {
            this.sourceMessage = sourceMessage;
        }

        @Override
        public boolean acknowledge() throws ConnectorException {
            if (sourceMessage == null)
                return true;
            return sourceMessage.acknowledge();
        }
    }
}
