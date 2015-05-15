package forklift.connectors;

import forklift.decorators.Headers;
import forklift.message.Header;

import java.util.Map;

import javax.jms.Message;

public class ForkliftMessage {
    private Message jmsMsg;
    private String msg;
    private boolean flagged;
    private String warning;
    private Map<Header, String> headers;
    private Map<String, Object> properties;

    public ForkliftMessage() {
    }

    public ForkliftMessage(Message jmsMsg) {
        this.jmsMsg = jmsMsg;
    }

    public Message getJmsMsg() {
        return jmsMsg;
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

    public void setHeaders(Map<Header, String> headers) {
        this.headers = headers;
    }

    public Map<Header, String> getHeaders() {
        return headers;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
