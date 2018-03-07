package forklift.message;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.KafkaSerializer;
import forklift.controller.KafkaController;
import forklift.producers.KafkaForkliftProducer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessage extends ForkliftMessage {
    private final KafkaController controller;
    private final ConsumerRecord<?, ?> consumerRecord;
    private final Generation latestGeneration;
    private final long createdGenerationNumber;

    public KafkaMessage(KafkaController controller, ConsumerRecord<?, ?> consumerRecord, Generation latestGeneration) {
        this.controller = controller;
        this.consumerRecord = consumerRecord;
        this.latestGeneration = latestGeneration;
        this.createdGenerationNumber = latestGeneration.generationNumber();

        createMessage();
    }

    public ConsumerRecord<?, ?> getConsumerRecord() {
        return this.consumerRecord;
    }

    @Override
    public boolean acknowledge() throws ConnectorException {
        if (!latestGeneration.acceptsGeneration(createdGenerationNumber)) { return false; }

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
    private final void createMessage() {
        String message = KafkaSerializer.extractMessageFromRecord(consumerRecord.value(), this.properties);
        if (message != null) {
            setMsg(message);
        }
        else{
            this.setFlagged(true);
            this.setWarning("Unable to parse message for topic: " + consumerRecord.topic() + " with value: " + consumerRecord.value());
        }
    }
}
