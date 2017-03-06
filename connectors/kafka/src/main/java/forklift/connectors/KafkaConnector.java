package forklift.connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.KafkaTopicConsumer;
import forklift.message.KafkaMessage;
import forklift.producers.ForkliftProducerI;
import forklift.producers.KafkaForkliftProducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Message;

/**
 * Manages a {@link org.apache.kafka.clients.consumer.KafkaConsumer}.  A subscription is made whenever  a call to getQueue or get Topic
 * is received.  Subscriptions are removed if no consumption is seen for
 */
public class KafkaConnector implements ForkliftConnectorI {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);
    private final String kafkaHosts;
    private final String schemaRegistries;
    private final String groupId;
    private KafkaConsumer<?, ?> kafkaConsumer;
    private KafkaProducer<?, ?> kafkaProducer;
    private MessageStream messageStream = new MessageStream();

    static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                   .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    KafkaController controller;

    public KafkaConnector(String kafkaHosts, String schemaRegistries, String groupId) {
        this.kafkaHosts = kafkaHosts;
        this.schemaRegistries = schemaRegistries;
        this.groupId = groupId;
    }

    @Override
    public void start() throws ConnectorException {

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        //schema.registry.url is a comma separated list of urls
        producerProperties.put("schema.registry.url", schemaRegistries);
        this.kafkaProducer = new KafkaProducer(producerProperties);
        this.controller = createController();
        this.controller.start();
    }

    private KafkaController createController() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHosts);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", false);
        props.put("consumer.timeout.ms", "-1");
        props.put("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistries);
        props.put("specific.avro.reader", false);
        //props.put("auto.offset.reset", "earliest");
        this.kafkaConsumer = new KafkaConsumer(props);
        this.controller = new KafkaController(kafkaConsumer, messageStream);
        return controller;
    }

    @Override
    public void stop() throws ConnectorException {
        try {
            this.controller.stop(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("KafkaConnector interrupted while stopping");
        }
        this.kafkaProducer.close();
    }

    @Override
    public Connection getConnection() throws ConnectorException {
        throw new UnsupportedOperationException("Kafka does not support Connections, please work with Queues and Topics");
    }

    @Override
    public ForkliftConsumerI getQueue(String name) throws ConnectorException {
        return this.getTopic(name);
    }

    @Override
    public ForkliftConsumerI getTopic(String name) throws ConnectorException {
        synchronized (this) {
            if (this.controller == null || !this.controller.isRunning()) {
                this.controller = createController();
                this.controller.start();
            }
        }
        return new KafkaTopicConsumer(name, controller, messageStream);
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        return this.getTopicProducer(name);
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
        return new KafkaForkliftProducer(name, this.kafkaProducer);
    }

    @Override
    public ForkliftMessage jmsToForklift(Message m) {
        try {
            final ForkliftMessage msg = new ForkliftMessage(m);
            if (m instanceof KafkaMessage) {
                KafkaMessage kafkaMessage = (KafkaMessage)m;
                msg.setProperties(kafkaMessage.getProperties());
                ConsumerRecord<?, ?> record = kafkaMessage.getConsumerRecord();
                if (record.value() instanceof GenericRecord) {
                    GenericRecord genericRecord = (GenericRecord)record.value();
                    Object value = genericRecord.get("forkliftMapMsg");
                    value = value == null ? genericRecord.get("forkliftMsg") : value;
                    value = value == null ? genericRecord.get("forkliftJsonMsg") : value;
                    if (value == null) {
                        String jsonValue = genericRecord.toString();
                        value = jsonValue != null && jsonValue.startsWith("{") ? jsonValue : value;
                    }
                    if (value == null) {
                        msg.setFlagged(true);
                        msg.setWarning("Unable to parse message for topic: " + record.topic() + " with value: " + record.value());
                    } else {
                        msg.setMsg(value.toString());
                    }
                } else {
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        //inefficient as we map from object to json, then back to the object.  This approach works
                        //without any changes to the forklift core libraries however.
                        msg.setMsg(mapper.writeValueAsString(record.value()));
                    } catch (JsonProcessingException e) {
                        msg.setFlagged(true);
                        msg.setWarning("Unable to parse object to json for topic: " +
                                       record.topic() +
                                       " with value: " +
                                       record.value());
                    }
                }
            } else {
                msg.setFlagged(true);
                msg.setWarning("Unexpected message type: " + m.getClass().getName());
            }

            return msg;
        } catch (Exception e) {
            return null;
        }
    }
}
