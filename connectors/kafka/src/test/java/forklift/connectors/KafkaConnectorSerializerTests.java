package forklift.connectors;

import forklift.Forklift;
import forklift.integration.server.TestServiceManager;
import forklift.source.sources.TopicSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KafkaConnectorSerializerTests {
    private static TestServiceManager serviceManager;
    private static ForkliftSerializer connectorSerializer;
    private static final TopicSource testSource = new TopicSource("test-string-topic");

    @BeforeAll
    public static void setup() throws Exception {
        serviceManager = new TestServiceManager();
        serviceManager.start();

        Forklift forklift = serviceManager.newManagedForkliftInstance();
        connectorSerializer = forklift.getConnector().getDefaultSerializer();
    }

    @AfterAll
    public static void teardown() {
        serviceManager.stop();
    }

    private byte[] subbytes(byte[] bytes, int start, int end) {
        byte[] result = new byte[end - start];
        System.arraycopy(bytes, start, result, 0, end - start);
        return result;
    }

    // the confluent avro serializer writes 1 magic byte (0x00)
    // followed by 4 bytes of the id of the schema to deserialize
    // the following avro data
    private byte[] stripHeader(byte[] bytes) {
        return subbytes(bytes, 5, bytes.length);
    }

    private void assertBytesEqual(byte[] expected, byte[] actual) {
        assertArrayEquals(
            expected, actual, "Expected bytes '" + hexString(expected) +
            "', found bytes '" + hexString(actual) + "'");
    }

    private static final char[] hexChar = "0123456789ABCDEF".toCharArray();
    private String hexString(byte[] bytes) {
        char[] result = new char[2 * bytes.length];

        for (int i = 0; i < bytes.length; i++) {
            result[2 * i] = hexChar[bytes[i] & 0x0F];
            result[2 * i + 1] = hexChar[(bytes[i] >> 4) & 0x0F];
        }

        return new String(result);
    }

    @Test
    public void testSampleStringSerializesCorrectly() {
        final byte[] sampleStringSerialized = connectorSerializer.serializeForSource(testSource, "hello");
        final byte[] sampleStringAvro = stripHeader(sampleStringSerialized);
        final byte[] expectedSampleStringAvro = new byte[]{
            10, // double the length of the string
            'h', 'e', 'l', 'l', 'o',
            0 // double the length of the properties (none supplied)
        };


        assertBytesEqual(expectedSampleStringAvro, sampleStringAvro);
    }
}
