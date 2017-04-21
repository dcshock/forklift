package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.KafkaTopicConsumer;
import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import forklift.producers.ForkliftProducerI;
import forklift.producers.KafkaForkliftProducer;
import forklift.source.QueueSource;
import forklift.source.TopicSource;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Manages both consuming and producing events on the kafka message broker.
 */
public class KafkaConnector implements ForkliftConnectorI {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);

    private final String kafkaHosts;
    private final String schemaRegistries;
    private final String groupId;

    private KafkaProducer<?, ?> kafkaProducer;
    private KafkaController controller;

    /**
     * Constructs a new instance of the KafkaConnector
     *
     * @param kafkaHosts       list of kafka servers in host:port,... format
     * @param schemaRegistries list of schema registry servers in http://host:port,... format
     * @param groupId          the groupId to use when subscribing to topics
     */
    public KafkaConnector(String kafkaHosts, String schemaRegistries, String groupId) {
        this.kafkaHosts = kafkaHosts;
        this.schemaRegistries = schemaRegistries;
        this.groupId = groupId;
    }

    @Override
    public void start() throws ConnectorException {
        //We do nothing here.  Consumer and producer are created when needed
    }

    private KafkaProducer createKafkaProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistries);

        return new KafkaProducer(producerProperties);
    }

    private KafkaController createController() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistries);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        final KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer(props);
        return new KafkaController(kafkaConsumer, new MessageStream());
    }

    @Override
    public synchronized void stop() throws ConnectorException {
        try {
            if (controller != null) {
                controller.stop(2000, TimeUnit.MILLISECONDS);
                controller = null;
            }
        } catch (InterruptedException e) {
            log.error("KafkaConnector interrupted while stopping");
        }
        if (kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }
    }

    @Override
    public ForkliftConsumerI getQueue(String name) throws ConnectorException {
        return getTopic(name);
    }

    @Override
    public ForkliftConsumerI consumeFromSource(ConsumerSource source) throws ConnectorException {
        return source
            .apply(QueueSource.class, queue -> getQueue(queue.getName()))
            .apply(TopicSource.class, topic -> getTopic(topic.getName()))
            .elseUnsupported();
    }

    @Override
    public synchronized ForkliftConsumerI getTopic(String name) throws ConnectorException {
        if (controller == null || !controller.isRunning()) {
            controller = createController();
            controller.start();
        }
        return new KafkaTopicConsumer(name, controller);
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        return getTopicProducer(name);
    }

    @Override
    public synchronized ForkliftProducerI getTopicProducer(String name) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        return new KafkaForkliftProducer(name, kafkaProducer);
    }

    @Override
    public boolean supportsResponse() {
        return true;
    }

    @Override
    public boolean supportsTopic() {
        return true;
    }

    @Override
    public boolean supportsQueue() {
        return true;
    }
}
