package forklift.connectors;

import javax.jms.Message;

public class ForkliftMessage {
    private Message jmsMsg;
    private String msg;
    private boolean flagged;
    private String warning;

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

}
