package forklift.connectors;

import java.util.HashMap;
import java.util.Map;

public class ForkliftMessage {
    private String id;
    protected String msg;
    protected boolean flagged;
    protected String warning;
    protected Map<String, String> properties = new HashMap<>();

    public ForkliftMessage() {
    }

    public ForkliftMessage(String msg) {
        this.setMsg(msg);
    }

    public boolean acknowledge() throws ConnectorException {
        return true;
    }

    public String getId() {
        return null;
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
}
