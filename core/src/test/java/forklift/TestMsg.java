package forklift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

public class TestMsg implements Message {
    private static final Logger log = LoggerFactory.getLogger(TestMsg.class);

    private String corrId;

    public TestMsg(String id) {
        this.corrId = id;
    }

    @Override
    public void acknowledge() throws JMSException {
        log.debug("ack id " + corrId);
    }
    @Override
    public void clearBody() throws JMSException { }
    @Override
    public void clearProperties() throws JMSException { }
    @Override
    public boolean getBooleanProperty(String arg0) throws JMSException { return false; }
    @Override
    public byte getByteProperty(String arg0) throws JMSException { return 0; }
    @Override
    public double getDoubleProperty(String arg0) throws JMSException {	return 0; }
    @Override
    public float getFloatProperty(String arg0) throws JMSException { return 0; }
    @Override
    public int getIntProperty(String arg0) throws JMSException { return 0; }
    @Override
    public String getJMSCorrelationID() throws JMSException { return corrId; }
    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException { return null; }
    @Override
    public int getJMSDeliveryMode() throws JMSException { return 0; }
    @Override
    public Destination getJMSDestination() throws JMSException { return null; }
    @Override
    public long getJMSExpiration() throws JMSException { return 0; }
    @Override
    public String getJMSMessageID() throws JMSException { return null; }
    @Override
    public int getJMSPriority() throws JMSException { return 0; }
    @Override
    public boolean getJMSRedelivered() throws JMSException { return false; }
    @Override
    public Destination getJMSReplyTo() throws JMSException { return null; }
    @Override
    public long getJMSTimestamp() throws JMSException { return 0; }
    @Override
    public String getJMSType() throws JMSException { return null; }
    @Override
    public long getLongProperty(String arg0) throws JMSException { return 0; }
    @Override
    public Object getObjectProperty(String arg0) throws JMSException { return null; }
    @Override
    public Enumeration getPropertyNames() throws JMSException { return null; }
    @Override
    public short getShortProperty(String arg0) throws JMSException { return 0; }
    @Override
    public String getStringProperty(String arg0) throws JMSException { return null; }
    @Override
    public boolean propertyExists(String arg0) throws JMSException { return false; }
    @Override
    public void setBooleanProperty(String arg0, boolean arg1) throws JMSException { }
    @Override
    public void setByteProperty(String arg0, byte arg1) throws JMSException { }
    @Override
    public void setDoubleProperty(String arg0, double arg1) throws JMSException { }
    @Override
    public void setFloatProperty(String arg0, float arg1) throws JMSException { }
    @Override
    public void setIntProperty(String arg0, int arg1) throws JMSException { }
    @Override
    public void setJMSCorrelationID(String arg0) throws JMSException {
        corrId = arg0;
    }
    @Override
    public void setJMSCorrelationIDAsBytes(byte[] arg0) throws JMSException { }
    @Override
    public void setJMSDeliveryMode(int arg0) throws JMSException { }
    @Override
    public void setJMSDestination(Destination arg0) throws JMSException { }
    @Override
    public void setJMSExpiration(long arg0) throws JMSException { }
    @Override
    public void setJMSMessageID(String arg0) throws JMSException { }
    @Override
    public void setJMSPriority(int arg0) throws JMSException { }
    @Override
    public void setJMSRedelivered(boolean arg0) throws JMSException { }
    @Override
    public void setJMSReplyTo(Destination arg0) throws JMSException { }
    @Override
    public void setJMSTimestamp(long arg0) throws JMSException { }
    @Override
    public void setJMSType(String arg0) throws JMSException { }
    @Override
    public void setLongProperty(String arg0, long arg1) throws JMSException { }
    @Override
    public void setObjectProperty(String arg0, Object arg1) throws JMSException { }
    @Override
    public void setShortProperty(String arg0, short arg1) throws JMSException { }
    @Override
    public void setStringProperty(String arg0, String arg1) throws JMSException { }
    @Override
    public String toString() {
        return corrId;
    }
}
