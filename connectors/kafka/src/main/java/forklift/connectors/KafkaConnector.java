package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.KafkaTopicConsumer;
import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import forklift.producers.ForkliftProducerI;
import forklift.producers.KafkaForkliftProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Manages a {@link org.apache.kafka.clients.consumer.KafkaConsumer}.  A subscription is made whenever  a call to getQueue or get Topic
 * is received.  Subscriptions are removed if no consumption is seen for
 */
public class KafkaConnector implements ForkliftConnectorI {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);
    private final String kafkaHosts;
    private final String schemaRegistries;
    private final String groupId;
    private KafkaProducer<?, ?> kafkaProducer;
    private MessageStream messageStream = new MessageStream();
    private KafkaController controller;

    /**
     * Constructs a nw instance of the KafkaConnector
     *
     * @param kafkaHosts list of kafka servers in host:port,... format
     * @param schemaRegistries list of schema registry servers in http://host:port,... format
     * @param groupId the groupId to use when subscribing to topics
     */
    public KafkaConnector(String kafkaHosts, String schemaRegistries, String groupId) {
        this.kafkaHosts = kafkaHosts;
        this.schemaRegistries = schemaRegistries;
        this.groupId = groupId;
    }

    @Override
    public void start() throws ConnectorException {
        this.controller = createController();
        this.controller.start();
    }

    private KafkaProducer createKafkaProducer() {

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        //schema.registry.url is a comma separated list of urls
        producerProperties.put("schema.registry.url", schemaRegistries);
        kafkaProducer = new KafkaProducer(producerProperties);
        return kafkaProducer;
    }

    private KafkaController createController() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHosts);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistries);
        props.put("specific.avro.reader", false);
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "200");
        KafkaConsumer<?, ?> kafkaConsumer;
        kafkaConsumer = new KafkaConsumer(props);
        this.controller = new KafkaController(kafkaConsumer, messageStream);
        return controller;
    }

    @Override
    public void stop() throws ConnectorException {
        try {
            if (controller != null) {
                this.controller.stop(2000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            log.error("KafkaConnector interrupted while stopping");
        }
        if (kafkaProducer != null) {
            this.kafkaProducer.close();
        }
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
        return new KafkaTopicConsumer(name, controller);
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        return this.getTopicProducer(name);
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
        synchronized (this) {
            if (this.kafkaProducer == null) {
                this.kafkaProducer = createKafkaProducer();
            }
        }
        return new KafkaForkliftProducer(name, this.kafkaProducer);
    }
}
