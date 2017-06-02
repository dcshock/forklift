package forklift.connectors;

import forklift.consumer.parser.KeyValueParser;
import forklift.producers.KafkaForkliftProducer;
import forklift.source.SourceI;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaSerializer implements ForkliftSerializer {
    private static final Logger log = LoggerFactory.getLogger(KafkaSerializer.class);

    private KafkaConnector connector;
    private Serializer serializer;
    private Deserializer deserializer;

    public final static String SCHEMA_FIELD_NAME_VALUE = "forkliftValue";
    public final static String SCHEMA_FIELD_NAME_PROPERTIES = "forkliftProperties";
    public static final Schema FORKLIFT_SCHEMA = readSchemaFromClasspath("schemas/ForkliftMessage.avsc");
    public static final String SCHEMA_FIELD_VALUE_PROPERTIES =
                    "{\"name\":\"forkliftProperties\",\"type\":\"string\",\"default\":\"\"," +
                    "\"doc\":\"Properties added to support forklift interfaces. Format is key,value entries delimited with new lines\"}";

    private static Schema readSchemaFromClasspath(String path) {
        Schema.Parser parser = new Schema.Parser();
        try {
            return parser.parse(Thread.currentThread().getContextClassLoader().getResourceAsStream(path));
        } catch (Exception e) {
            log.error("Couldn't parse forklift schema", e);
        }
        return null;
    }

    public KafkaSerializer(KafkaConnector connector, Serializer serializer, Deserializer deserializer) {
        this.connector = connector;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public byte[] serializeForSource(SourceI source, Object o) {
        final String topicName = source
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .apply(RoleInputSource.class, roleSource -> connector.mapRoleInputSource(roleSource).getName())
            .elseUnsupportedError();

        if (o instanceof String) {
            GenericRecord avroRecord = new GenericData.Record(FORKLIFT_SCHEMA);
            avroRecord.put(SCHEMA_FIELD_NAME_VALUE, (String) o);
            avroRecord.put(SCHEMA_FIELD_NAME_PROPERTIES, "");

            o = avroRecord;
        }
        return serializer.serialize(topicName, o);
    }

    @Override
    public <T> T deserializeForSource(SourceI source, byte[] bytes) {
        final String topicName = source
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .apply(RoleInputSource.class, roleSource -> connector.mapRoleInputSource(roleSource).getName())
            .elseUnsupportedError();

        return (T) extractMessageFromRecord(deserializer.deserialize(topicName, bytes), null);
    }

    /**
     * A utility for pulling the message and properties out of a record deserialized
     * off of Kafka.
     *
     * @param <T> the type of the record value
     * @param recordValue the value of the record to extract from
     * @param properties the map to add extracted properties to, or null
     *     if no properties are needed
     * @return the message stored in the record value
     */
    public static <T> String extractMessageFromRecord(T recordValue, Map<String, String> properties) {
        Object value = null;
        if (recordValue instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) recordValue;
            if (properties != null) {
                Object messageProperties = genericRecord.get(SCHEMA_FIELD_NAME_PROPERTIES);
                if (messageProperties != null) {
                    properties.putAll(KeyValueParser.parse(messageProperties.toString()));
                }
            }

            value = genericRecord.get(KafkaSerializer.SCHEMA_FIELD_NAME_VALUE);
            //If the value is null, this is most likely an avro object
            if (value == null) {
                String jsonValue = genericRecord.toString();
                value = jsonValue != null && jsonValue.startsWith("{") ? jsonValue : null;
            }
        }
        return value == null?null:value.toString();
    }
}
