package forklift.message;

import java.lang.reflect.Type;

public enum Header {
    /**
     * The standard message id
     * <ul>
     * <li> JMS: JMSCorrelationID
     * <li> Stomp: "correlation-id"
     * <li> Amqp: "properties.correlation-id"
     * <li> Type: String
     * </ul>
     */
    CorrelationId("JMSCorrelationID", "correlation-id", "properties.correlation-id", String.class),
    /**
     * The message delivery count. If a message is not properly acknowledged by a consumer, it may
     * be re-delivered. This property keeps a total of the number of times the broker attempts to
     * deliver a message to a consumer.
     * <ul>
     * <li> JMS: JMSXDeliveryCount
     * <li> Stomp: "JMSXDeliveryCount"
     * <li> Amqp: "header.durable"
     * <li> Type: Integer
     * </ul>
     */
    DeliveryCount("JMSXDeliveryCount", "JMSXDeliveryCount", "header.deliveryCount", Integer.class),
    /**
     * The message delivery mode. Guarantees broker storage when persistent.
     * <ul>
     * <li> JMS: JMSDeliveryMode
     * <li> Stomp: "persistent"
     * <li> Amqp: "header.durable"
     * <li> Type: Integer - 1: non-persistent, 2: persistent;
     * </ul>
     */
    DeliveryMode("JMSDeliveryMode", "persistent", "header.durable", int.class),
    /**
     * The message expiration
     * <ul>
     * <li> JMS: JMSExpiration
     * <li> Stomp: "expires"
     * <li> Amqp: "header.ttl"
     * <li> Type: Long
     * <li> Note: Time in milliseconds - 0 means never expire
     * </ul>
     */
    Expiration("JMSExpiration", "expires", "header.ttl", long.class),
    /**
     * The message group id
     * <ul>
     * <li> JMS: JMSXGroupID
     * <li> Stomp: "JMSXGroupID"
     * <li> Amqp: "application-properties.JMSXGroupID"
     * <li> Type: String
     * </ul>
     */
    GroupId("JMSXGroupID", "JMSXGroupID", "application-properties.JMSXGroupID", String.class),
    /**
     * Specifies the sequence number in a Message Group
     * <ul>
     * <li> JMS: JMSXGroupSeq
     * <li> Stomp: "JMSXGroupSeq"
     * <li> Amqp: "application-properties.JMSXGroupSeq"
     * <li> Type: Integer
     * </ul>
     */
    GroupSeq("JMSXGroupSeq", "JMSXGroupSeq", "application-properties.JMSXGroupSequence", Integer.class),
    /**
     * The previous message's destination. Used to record which queue a message was
     * initially bound when being audited.
     * <ul>
     * <li> JMS: JMSPPreviousDestination
     * <li> Stomp: "previous-destination"
     * <li> Amqp: "application-properties.previous-destination"
     * <li> Type: String
     * </ul>
     */
    PreviousDestination("JMSPPreviousDestination", "previous-destination", "application-properties.previous-destination", String.class),
    /**
     * The message priority
     * <ul>
     * <li> JMS: JMSPriority
     * <li> Stomp: "priority"
     * <li> Amqp: "header.priority"
     * <li> Type: Integer
     * <li> Note: 0 is lowest, 9 is highest
     * </ul>
     */
    Priority("JMSPriority", "priority", "header.priority", int.class),
    /**
     * The message producer's name
     * <ul>
     * <li> JMS: JMSPProducer
     * <li> Stomp: "producer"
     * <li> Amqp: "application-properties.producer"
     * <li> Type: String
     * </ul>
     */
    Producer("JMSPProducer", "producer", "application-properties.producer", String.class),
    /**
     * Destination the consumer should send replies. Should be a URI. If the implementation of the JMS
     * provider does not support type String, use property.
     * <ul>
     * <li> JMS: JMSReplyTo or JMSPReplyTo
     * <li> Stomp: "reply-to"
     * <li> Amqp: "properties.reply-to"
     * <li> Type: String
     * </ul>
     */
    ReplyTo("JMSReplyTo", "reply-to", "properties.reply-to", String.class),
    /**
     * The result of processing a message, added to messages sent to specialized consumers for auditing.
     * <ul>
     * <li> JMS: JMSPResult
     * <li> Stomp: "result"
     * <li> Amqp: "application-properties.result"
     * <li> Type: String
     * </ul>
     */
    Result("JMSPResult", "result", "application-properties.result", String.class),
    /**
     * The detail about a result of processing a message, added to messages sent to specialized consumers for auditing.
     * <ul>
     * <li> JMS: JMSPResultDetail
     * <li> Stomp: "result-detail"
     * <li> Amqp: "application-properties.result-detail"
     * <li> Type: String
     * </ul>
     */
    ResultDetail("JMSPResultDetail", "result-detail", "application-properties.result-detail", String.class),
    /**
     * The number of times a message has tried to process
     * <ul>
     * <li> JMS: JMSPRetryCount
     * <li> Stomp: "retry-count"
     * <li> Amqp: "application-properties.retry-count"
     * <li> Type: Integer
     * </ul>
     */
    RetryCount("JMSPRetryCount", "retry-count", "application-properties.retry-count", Integer.class),
    /**
     * The message creation time
     * <ul>
     * <li> JMS: JMSTimestamp
     * <li> Stomp: "timestamp"
     * <li> Amqp: "properties.creation-time"
     * <li> Type: Long
     * </ul>
     */
    Timestamp("JMSTimestamp", "timestamp", "properties.creation-time", long.class),
    /**
     * The message type
     * <ul>
     * <li> JMS: JMSType
     * <li> Stomp: "type"
     * <li> Amqp: "message-annotations.x-opt-jms-type"
     * <li> Type: String
     * </ul>
     */
    Type("JMSType", "type", "message-annotations.x-opt-jms-type", String.class);


    private final String jmsMessage;
    private final String stompMessage;
    private final String amqpMessage;
    private final Type headerType;
    private Header(String jmsMessage, String stompMessage, String amqpMessage, Type headerType) {
        this.jmsMessage = jmsMessage;
        this.stompMessage = stompMessage;
        this.amqpMessage = amqpMessage;
        this.headerType = headerType;
    }

    public String getJmsMessage() { return jmsMessage; }
    public String getStompMessage() { return stompMessage; }
    public String getAmqpMessage() { return amqpMessage; }
    public Type getHeaderType() { return headerType; }
}
