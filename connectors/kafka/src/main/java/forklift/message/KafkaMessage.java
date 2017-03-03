package forklift.message;

import forklift.connectors.KafkaController;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Created by afrieze on 2/28/17.
 */
public class KafkaMessage implements Message {
    private final KafkaController controller;
    private final ConsumerRecord<?, ?> consumerRecord;
    Map<String, Object> properties = new HashMap<>();

    public KafkaMessage(KafkaController controller, ConsumerRecord<?, ?> consumerRecord) {
        this.controller = controller;
        this.consumerRecord = consumerRecord;
    }

    public ConsumerRecord<?, ?> getConsumerRecord() {
        return this.consumerRecord;
    }

    @Override public String getJMSMessageID() throws JMSException {
        return null;
    }

    @Override public void setJMSMessageID(String id) throws JMSException {

    }

    @Override public long getJMSTimestamp() throws JMSException {
        return 0;
    }

    @Override public void setJMSTimestamp(long timestamp) throws JMSException {

    }

    @Override public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return new byte[0];
    }

    @Override public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {

    }

    @Override public void setJMSCorrelationID(String correlationID) throws JMSException {

    }

    @Override public String getJMSCorrelationID() throws JMSException {
        return null;
    }

    @Override public Destination getJMSReplyTo() throws JMSException {
        return null;
    }

    @Override public void setJMSReplyTo(Destination replyTo) throws JMSException {

    }

    @Override public Destination getJMSDestination() throws JMSException {
        return null;
    }

    @Override public void setJMSDestination(Destination destination) throws JMSException {

    }

    @Override public int getJMSDeliveryMode() throws JMSException {
        return 0;
    }

    @Override public void setJMSDeliveryMode(int deliveryMode) throws JMSException {

    }

    @Override public boolean getJMSRedelivered() throws JMSException {
        return false;
    }

    @Override public void setJMSRedelivered(boolean redelivered) throws JMSException {

    }

    @Override public String getJMSType() throws JMSException {
        return null;
    }

    @Override public void setJMSType(String type) throws JMSException {

    }

    @Override public long getJMSExpiration() throws JMSException {
        return 0;
    }

    @Override public void setJMSExpiration(long expiration) throws JMSException {

    }

    @Override public int getJMSPriority() throws JMSException {
        return 0;
    }

    @Override public void setJMSPriority(int priority) throws JMSException {

    }

    @Override public void clearProperties() throws JMSException {
        properties.clear();
    }

    @Override public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    @Override public boolean getBooleanProperty(String name) throws JMSException {
        return (boolean)properties.get(name);
    }

    @Override public byte getByteProperty(String name) throws JMSException {
        return (byte)properties.get(name);
    }

    @Override public short getShortProperty(String name) throws JMSException {
        return (short)properties.get(name);
    }

    @Override public int getIntProperty(String name) throws JMSException {
        return (int)properties.get(name);
    }

    @Override public long getLongProperty(String name) throws JMSException {
        return (long)properties.get(name);
    }

    @Override public float getFloatProperty(String name) throws JMSException {
        return (float)properties.get(name);
    }

    @Override public double getDoubleProperty(String name) throws JMSException {
        return (double)properties.get(name);
    }

    @Override public String getStringProperty(String name) throws JMSException {
        return (String)properties.get(name);
    }

    @Override public Object getObjectProperty(String name) throws JMSException {
        return properties.get(name);
    }

    @Override public Enumeration getPropertyNames() throws JMSException {
        return new Vector(properties.keySet()).elements();
    }

    @Override public void setBooleanProperty(String name, boolean value) throws JMSException {
        properties.put(name, value);
    }

    @Override public void setByteProperty(String name, byte value) throws JMSException {
        properties.put(name, value);
    }

    @Override public void setShortProperty(String name, short value) throws JMSException {
        properties.put(name, value);
    }

    @Override public void setIntProperty(String name, int value) throws JMSException {
        properties.put(name, value);

    }

    @Override public void setLongProperty(String name, long value) throws JMSException {
        properties.put(name, value);

    }

    @Override public void setFloatProperty(String name, float value) throws JMSException {
        properties.put(name, value);

    }

    @Override public void setDoubleProperty(String name, double value) throws JMSException {
        properties.put(name, value);

    }

    @Override public void setStringProperty(String name, String value) throws JMSException {
        properties.put(name, value);

    }

    @Override public void setObjectProperty(String name, Object value) throws JMSException {
        properties.put(name, value);

    }

    public Map<String, Object> getProperties(){
        return properties;
    }

    @Override public void acknowledge() throws JMSException {
        try {
            if(!controller.acknowledge(consumerRecord)){
                throw new JMSException("Unable to acknowledge message, possibly due to kafka partition rebalance", "KAFKA-REBALANCE");
            }
        } catch (InterruptedException e) {
            throw new JMSException("Error acknowledging message");
        }
    }

    @Override public void clearBody() throws JMSException {

    }
}
