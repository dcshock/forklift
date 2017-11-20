package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.KafkaTopicConsumer;
import forklift.consumer.wrapper.RoleInputConsumerWrapper;
import forklift.controller.KafkaController;
import forklift.decorators.Message;
import forklift.message.ForkliftAvroMessageUtils;
import forklift.message.MessageStream;
import forklift.producers.ForkliftProducerI;
import forklift.producers.KafkaForkliftProducer;
import forklift.serializers.ForkliftKafkaAvroDeserializer;
import forklift.source.ActionSource;
import forklift.source.LogicalSource;
import forklift.source.SourceI;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.TopicSource;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    private ForkliftSerializer serializer;
    private Map<String, KafkaController> controllers = new HashMap<>();

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

    private KafkaController createController(GroupedTopicSource source) {

        ForkliftKafkaAvroDeserializer deserializer = new ForkliftKafkaAvroDeserializer();
        Schema readerSchema = null;
        try {
            readerSchema = readerSchemaFromSource(source);
        } catch (Exception e) {
            log.error("Unable to generate reader schema, falling back to writer schema.  Defaults may be lost");
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ForkliftKafkaAvroDeserializer.class);
        if (readerSchema != null) {
            props.put(ForkliftKafkaAvroDeserializer.READER_SCHEMA_CONFIG, readerSchema);
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistries);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        final KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer(props);
        return new KafkaController(kafkaConsumer, new MessageStream(), source.getName());
    }

    private Schema readerSchemaFromSource(SourceI source) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        for (Field field : source.getContextClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(Message.class) && SpecificRecord.class.isAssignableFrom(field.getType())) {
                Schema schema = (Schema) field.getType().getMethod("getClassSchema").invoke(null);
                return ForkliftAvroMessageUtils.addForkliftPropertiesToSchema(schema);
            }
        }
        return null;
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
        return source
                   .apply(QueueSource.class, queue -> {
                       GroupedTopicSource groupedTopicSource = new GroupedTopicSource(queue.getName(), groupId);
                       groupedTopicSource.setContextClass(source.getContextClass());
                       ForkliftConsumerI consumerI = getGroupedTopic(groupedTopicSource);
                       return consumerI;
                   })
                   .apply(TopicSource.class, topic -> {
                       GroupedTopicSource groupedTopicSource = new GroupedTopicSource(topic.getName(), groupId);
                       groupedTopicSource.setContextClass(source.getContextClass());
                       ForkliftConsumerI consumerI = getGroupedTopic(groupedTopicSource);
                       return consumerI;
                   })
                   .apply(GroupedTopicSource.class, topic -> getGroupedTopic(topic))
                   .apply(RoleInputSource.class, roleSource -> {
                       final ForkliftConsumerI rawConsumer = getConsumerForSource(roleSource.getActionSource(this));
                       return new RoleInputConsumerWrapper(rawConsumer);
                   })
                   .elseUnsupportedError();
    }

    public synchronized ForkliftConsumerI getGroupedTopic(GroupedTopicSource source) throws ConnectorException {
        if (!source.groupSpecified()) {
            source.overrideGroup(groupId);
        }
        if (!source.getGroup().equals(groupId)) { //TODO actually support GroupedTopics
            throw new ConnectorException("Unexpected group '" + source.getGroup() + "'; only the connector group '" + groupId + "' is allowed");
        }

        KafkaController controller = controllers.get(source.getName());
        if (controller != null && controller.isRunning()) {
            log.warn("Consumer for topic already exists under this controller's groupname.  Messages will be divided amongst consumers.");
        } else {
            controller = createController(source);
            this.controllers.put(source.getName(), controller);
            controller.start();
        }
        return new KafkaTopicConsumer(source.getName(), controller);
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
        return new GroupedTopicSource("forklift-role-" + roleSource.getRole(), groupId);
    }

    @Override
    public boolean supportsResponse() {
        return true;
    }
}
