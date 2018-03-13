package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.KafkaTopicConsumer;
import forklift.consumer.wrapper.RoleInputConsumerWrapper;
import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import forklift.producers.ForkliftProducerI;
import forklift.producers.KafkaForkliftProducer;
import forklift.source.ActionSource;
import forklift.source.LogicalSource;
import forklift.source.SourceI;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Manages both consuming and producing events on the kafka message broker.
 */
public class KafkaConnector implements ForkliftConnectorI {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);

    private final String kafkaHosts;
    private final String schemaRegistries;
    private final String defaultGroupId;

    private KafkaProducer<?, ?> kafkaProducer;
    private KafkaController controller;
    private ForkliftSerializer serializer;
    private Map<GroupedTopicSource, KafkaController> controllers = new HashMap<>();

    /**
     * Constructs a new instance of the KafkaConnector
     *
     * @param kafkaHosts       list of kafka servers in host:port,... format
     * @param schemaRegistries list of schema registry servers in http://host:port,... format
     * @param defaultGroupId   the default groupId to use when subscribing to topics
     */
    public KafkaConnector(String kafkaHosts, String schemaRegistries, String defaultGroupId) {
        this.kafkaHosts = kafkaHosts;
        this.schemaRegistries = schemaRegistries;
        this.defaultGroupId = defaultGroupId;
        this.serializer = new KafkaSerializer(this, newSerializer(), newDeserializer());
    }

    private KafkaAvroSerializer newSerializer() {
        Map<String, Object> serializerProperties = new HashMap<>();
        serializerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistries);

        KafkaAvroSerializer result = new KafkaAvroSerializer();
        result.configure(serializerProperties, false);
        return result;
    }

    private KafkaAvroDeserializer newDeserializer() {
        Map<String, Object> deserializerProperties = new HashMap<>();
        deserializerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistries);
        deserializerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        KafkaAvroDeserializer result = new KafkaAvroDeserializer();
        result.configure(deserializerProperties, false);
        return result;
    }

    @Override
    public ForkliftSerializer getDefaultSerializer() {
        return serializer;
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

    private KafkaController createController(String topicName, String groupId) {
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
        return new KafkaController(kafkaConsumer, new MessageStream(), topicName);
    }

    @Override
    public synchronized void stop() throws ConnectorException {

        controllers.values().parallelStream().forEach(controller -> {
            try {
                controller.stop(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("KafkaConnector interrupted while stopping");
            }
        });
        controllers.clear();

        if (kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }
    }

    @Override
    public ForkliftConsumerI getConsumerForSource(SourceI source) throws ConnectorException {
        if (source instanceof RoleInputSource) {
            final RoleInputSource roleSource = (RoleInputSource) source;
            final ForkliftConsumerI rawConsumer = getConsumerForSource(roleSource.getActionSource(this));
            return new RoleInputConsumerWrapper(rawConsumer);
        }
        return getGroupedTopic(mapToGroupedTopic(source));
    }

    public synchronized ForkliftConsumerI getGroupedTopic(GroupedTopicSource source) throws ConnectorException {
        if (!source.groupSpecified()) {
            source.overrideGroup(defaultGroupId);
        }

        KafkaController controller = controllers.get(source);
        if (controller != null && controller.isRunning()) {
            log.warn("Consumer for topic and group already exists.  Messages will be divided amongst consumers.");
        } else {
            controller = createController(source.getName(), source.getGroup());
            this.controllers.put(source, controller);
            controller.start();
        }

        return new KafkaTopicConsumer(source.getName(), controller);
    }

    @Override
    public ForkliftConsumerI getQueue(String name) throws ConnectorException {
        return getGroupedTopic(mapToGroupedTopic(new QueueSource(name)));
    }

    @Override
    public ForkliftConsumerI getTopic(String name) throws ConnectorException {
        return getGroupedTopic(mapToGroupedTopic(new TopicSource(name)));
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
    public ActionSource mapSource(LogicalSource source) {
        return source
            .apply(RoleInputSource.class, roleSource -> mapRoleInputSource(roleSource))
            .get();
    }

    protected GroupedTopicSource mapRoleInputSource(RoleInputSource roleSource) {
        return new GroupedTopicSource("forklift-role-" + roleSource.getRole(), defaultGroupId);
    }

    @Override
    public boolean supportsResponse() {
        return true;
    }

    /* visible for testing */
    protected GroupedTopicSource mapToGroupedTopic(SourceI source) {
        return source
            .apply(QueueSource.class, queueSource -> new GroupedTopicSource(queueSource.getName(), defaultGroupId))
            .apply(TopicSource.class, topicSource -> topicToGroupedTopic(topicSource))
            .apply(GroupedTopicSource.class, groupedTopicSource -> groupedTopicSource)
            .elseUnsupportedError();
    }

    private GroupedTopicSource topicToGroupedTopic(TopicSource topicSource) {
        if (topicSource.getContextClass() == null) {
            return new GroupedTopicSource(topicSource.getName(), defaultGroupId);
        }
        final String groupName = defaultGroupId + "-" + topicSource.getContextClass().getSimpleName();
        return new GroupedTopicSource(topicSource.getName(), groupName);
    }
}
