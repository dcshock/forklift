package forklift.serializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Extends the {@link io.confluent.kafka.serializers.KafkaAvroDeserializer} in order to look for a reader schema
 * to use while deserializing messages.  This is done to override the default functionality of using the writer schema
 * to deserialize GenericRecords.  Using the writer schema when a reader schema is available has the potential of missing
 * out on default values specified in the reader schema.  A reader schema instance should be specified using the configuration
 * key of {@link #READER_SCHEMA_CONFIG}
 */
public class ForkliftKafkaAvroDeserializer extends KafkaAvroDeserializer implements Deserializer<Object> {

    public static final String READER_SCHEMA_CONFIG = "forklift.avro.reader.schema";
    private Schema readerSchema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        readerSchema = (Schema) configs.getOrDefault(READER_SCHEMA_CONFIG, null);
        super.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (readerSchema != null) {
            return deserialize(data, readerSchema);
        }
        return super.deserialize(topic, data);
    }

    @Override
    public void close() {
        super.close();
    }
}
