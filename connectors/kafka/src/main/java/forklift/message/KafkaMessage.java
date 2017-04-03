package forklift.message;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.controller.KafkaController;
import forklift.producers.KafkaForkliftProducer;

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
        Object value = null;
        if (consumerRecord.value() instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord)consumerRecord.value();
            Object properties = genericRecord.get(KafkaForkliftProducer.SCHEMA_FIELD_NAME_PROPERTIES);
            if (properties != null) {
                this.setProperties(KeyValueParser.parse(properties.toString()));
            }
            value = genericRecord.get(KafkaForkliftProducer.SCHEMA_FIELD_NAME_VALUE);
            //If the value is null, this is most likely an avro object
            if (value == null) {
                String jsonValue = genericRecord.toString();
                value = jsonValue != null && jsonValue.startsWith("{") ? jsonValue : null;
            }
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
    }
}
