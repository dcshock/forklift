package forklift.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.controller.KafkaController;
import forklift.consumer.parser.KeyValueParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessage extends ForkliftMessage {
    private final KafkaController controller;
    private final ConsumerRecord<?, ?> consumerRecord;

    public KafkaMessage(KafkaController controller, ConsumerRecord<?, ?> consumerRecord) {
        this.controller = controller;
        this.consumerRecord = consumerRecord;
        parseRecord();
    }

    public ConsumerRecord<?, ?> getConsumerRecord() {
        return this.consumerRecord;
    }

    @Override
    public boolean acknowledge() throws ConnectorException {
        try {
            return controller.acknowledge(consumerRecord);
        } catch (InterruptedException e) {
            throw new ConnectorException("Error acknowledging message");
        }
    }

    @Override
    public String getId() {
        return consumerRecord.topic() + "-" + consumerRecord.partition() + "-" + consumerRecord.offset();
    }

    /**
     * <strong>WARNING:</strong> Called from constructor
     */
    private final void parseRecord() {
        if (consumerRecord.value() instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord)consumerRecord.value();
            Object properties = genericRecord.get("forkliftProperties");
            if (properties != null) {
                this.setProperties(KeyValueParser.parse(properties.toString()));
            }
            Object value = genericRecord.get("forkliftMapMsg");
            value = value == null ? genericRecord.get("forkliftMsg") : value;
            value = value == null ? genericRecord.get("forkliftJsonMsg") : value;
            if (value == null) {
                String jsonValue = genericRecord.toString();
                value = jsonValue != null && jsonValue.startsWith("{") ? jsonValue : value;
            }
            if (value == null) {
                this.setFlagged(true);
                this.setWarning("Unable to parse message for topic: " +
                                consumerRecord.topic() +
                                " with value: " +
                                consumerRecord.value());
            } else {
                this.setMsg(value.toString());
            }
        } else {
            ObjectMapper mapper = new ObjectMapper();
            try {
                //inefficient as we map from object to json, then back to the object.  This approach works
                //without any changes to the forklift core libraries however.
                this.setMsg(mapper.writeValueAsString(consumerRecord.value()));
            } catch (JsonProcessingException e) {
                this.setFlagged(true);
                this.setWarning("Unable to parse object to json for topic: " +
                                consumerRecord.topic() +
                                " with value: " +
                                consumerRecord.value());
            }
        }
    }
}
