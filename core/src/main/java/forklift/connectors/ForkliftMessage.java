package forklift.connectors;

import forklift.message.Header;

import java.util.HashMap;
import java.util.Map;

/**
 * A class that encapsulates a message to retrieve or send on a message bus
 * and its various metadata.
 */
public class ForkliftMessage {
    private String id;
    protected String msg;
    protected boolean flagged;
    protected String warning;
    protected Map<String, String> properties = new HashMap<>();
    protected Map<Header, Object> headers = new HashMap<>();

    public ForkliftMessage() {
    }

    public ForkliftMessage(String msg) {
        this.setMsg(msg);
    }

    public boolean acknowledge() throws ConnectorException {
        return true;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    public boolean isFlagged() {
        return flagged;
    }

    public void setFlagged(boolean flagged) {
        this.flagged = flagged;
    }

    public void setProperties(Map<String, String> properties) {
        // Get rid of unmodifiable.
        final Map<String, String> newProps = new HashMap<>();
        properties.keySet().stream().forEach(key -> newProps.put(key, properties.get(key)));
        this.properties = newProps;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void setHeaders(Map<Header, Object> headers) {
         final Map<Header, Object> newHeaders = new HashMap<>();
         headers.keySet().stream().forEach(key -> newHeaders.put(key, headers.get(key)));
         this.headers = newHeaders;
    }

    public Map<Header, Object> getHeaders() {
        return this.headers;
    }
}
