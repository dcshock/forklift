package forklift.connectors;

import forklift.message.Header;

import java.util.Map;

import javax.jms.Message;

public class ForkliftMessage {
    private Message jmsMsg;
    private String msg;
    private boolean flagged;
    private String warning;
    private Map<Header, Object> headers;
    private Map<String, Object> properties;

    public ForkliftMessage() {
    }

    public ForkliftMessage(Message jmsMsg) {
        this.jmsMsg = jmsMsg;
    }

    public ForkliftMessage(String msg) {
        this.setMsg(msg);
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

    public void setHeaders(Map<Header, Object> headers) {
        this.headers = headers;
    }

    public Map<Header, Object> getHeaders() {
        return headers;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
