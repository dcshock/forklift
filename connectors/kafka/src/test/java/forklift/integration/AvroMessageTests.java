package forklift.integration;

import static org.junit.Assert.assertEquals;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.schemas.AvroMessage;
import forklift.schemas.StateCode;
import forklift.schemas.UserRegistered;
import forklift.source.decorators.Queue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class AvroMessageTests extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageTests.class);

    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
    }

    @Test
    public void testComplexAvroMessageWithProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
            producer =
            forklift.getConnector().getQueueProducer("forklift-avro-topic");
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("Eye", "producerProperty");
        producer.setProperties(producerProps);
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            timeouts++;

            if (sentMessageIds.equals(consumedMessageIds) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        // Start the consumer.
        c.listen();
        messageAsserts();
    }

    @Test
    public void testComplexAvroMessageWithoutProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
            producer =
            forklift.getConnector().getQueueProducer("forklift-avro-topic");
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            timeouts++;
            if (sentMessageIds.equals(consumedMessageIds) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        // Start the consumer.
        c.listen();
        messageAsserts();
    }

    @Test
    public void defaultValueInReaderSchemaAppropriatelySetWhenNotInWriterSchema() throws InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        String writerSchemaWithoutColor = "{\n" +
                                          "  \"namespace\": \"forklift.schemas\",\n" +
                                          "  \"type\": \"record\",\n" +
                                          "  \"version\": 1,\n" +
                                          "  \"name\": \"AvroMessage\",\n" +
                                          "  \"fields\": [\n" +
                                          "    {\"name\": \"name\", \"type\": \"string\"}\n" +
                                          "  ]\n" +
                                          "}";

        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        kafkaProps.put("bootstrap.servers", serviceManager.kafkaHost() + ":" + serviceManager.kafkaPort());
        kafkaProps.put("schema.registry.url", "http://" + serviceManager.schemaRegistryHost() + ":" + serviceManager.schemaRegistryPort());
        kafkaProps.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaProps);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(writerSchemaWithoutColor);
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "John");
        ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>("forklift-avro-default-test-topic", null, record);
        producer.send(data);

        final Consumer c = new Consumer(AvroMessageConsumer.class, forklift);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> c.listen());
        AvroMessage message = AvroMessageConsumer.resultHandoff.poll(5, TimeUnit.SECONDS);
        c.shutdown();
        assertEquals("red", message.getColor());

    }

    @Queue(value = "forklift-avro-default-test-topic")
    public static class AvroMessageConsumer {

        public static SynchronousQueue<AvroMessage> resultHandoff = new SynchronousQueue<AvroMessage>();

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private AvroMessage value;

        @OnMessage
        public void onMessage() throws InterruptedException {
            resultHandoff.put(value);
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }

    @Queue(value = "forklift-avro-topic")
    public static class RegisteredAvroConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private UserRegistered value;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            System.out.println(Thread.currentThread().getName() + value.getState());
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }
}
