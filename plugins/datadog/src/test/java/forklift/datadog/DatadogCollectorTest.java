package forklift.datadog;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class DatadogCollectorTest {

    @Test
    public void testConstructor() {
        // Only a null apiKey should throw an error otherwise the collector can be instantiated.
        assertThrows(IllegalArgumentException.class, () -> {
            new DatadogCollector(null, null, null, null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            new DatadogCollector("", null, null, null);
        });
        new DatadogCollector("a", null, null, null);
    }
}
