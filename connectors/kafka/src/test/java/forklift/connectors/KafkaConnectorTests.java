package forklift.connectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

public final class KafkaConnectorTests {
    private final KafkaConnector connector;

    public KafkaConnectorTests() {
        this.connector = new KafkaConnector("fake", "fake", "foo");
    }

    @Test
    public void testConsumerPropertiesOverride() {
        final Map<Object, Object> addedProperties = new HashMap<>();
        addedProperties.put("foo", "bar");
        addedProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "override");

        connector.setAddedConsumerProperties(addedProperties);

        final Properties props = connector.getConsumerProperties();
        assertEquals("bar", props.get("foo"));
        assertEquals("override", props.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public void testProducerPropertiesOverride() {
        final Map<Object, Object> addedProperties = new HashMap<>();
        addedProperties.put("foo", "bar");
        addedProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "override");

        connector.setAddedProducerProperties(addedProperties);

        final Properties props = connector.getProducerProperties();
        assertEquals("bar", props.get("foo"));
        assertEquals("override", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}
