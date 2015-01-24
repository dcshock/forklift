package forklift.connectors;

import javax.jms.Message;

public class ForkliftMessage {
	private Message jmsMsg;
	private String msg;
	
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
}
