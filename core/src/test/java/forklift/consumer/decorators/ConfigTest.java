

package forklift.consumer.decorators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.LifeCycleMonitors;
import forklift.decorators.Config;
import forklift.properties.PropertiesManager;
import forklift.source.decorators.Queue;

import java.io.File;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConfigTest {
    private static final String CONF = "configTestProperties";

    private static File file;
    private static PropertiesManager pm;
    private static Forklift forklift;
    private static ForkliftConnectorI connector;

    @BeforeAll
    public static void setUp() {
        file = new File(Thread.currentThread().getContextClassLoader().getResource(CONF + ".properties").getPath());
        pm = new PropertiesManager();
        pm.register(file);
        LifeCycleMonitors lifeCycle = new LifeCycleMonitors();
        forklift = mock(Forklift.class);
        connector = mock(ForkliftConnectorI.class);
        when(forklift.getLifeCycle()).thenReturn(lifeCycle);
        when(forklift.getConnector()).thenReturn(connector);
    }

    @AfterAll
    public static void tearDown() {
        pm.deregister(file);
    }

    @Test
    public void testConfigInjection() {
        Consumer test = new Consumer(TestConsumer.class, forklift, this.getClass().getClassLoader());
        TestConsumer tc = new TestConsumer();
        final ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        test.inject(msg, tc);
        assertEquals(tc.all.get("value"), "a");
        assertEquals(tc.configTestProperties.get("value"), "a");
        assertEquals(tc.value, "a");
        assertNull(tc.doesnotexist);
        assertEquals(tc.specific, "a");
        assertNull(tc.empty);
        assertEquals(tc.overridden, "a");
        assertEquals(tc.defaulted, "b");
    }

    // Test class for testing @On annotation
    // Different input messages result in different results
    @Queue("1")
    public class TestConsumer {
        @Config
        Properties configTestProperties;

        @Config(CONF)
        Properties all;

        @Config(CONF)
        String value;

        @Config(CONF)
        String doesnotexist;

        @Config(value=CONF, field="value")
        String specific;

        @Config(value=CONF, field="DoesNotExist")
        String empty;

        @Config(value=CONF, field="value")
        String overridden = "b";

        @Config(value=CONF, field="DoesNotExist")
        String defaulted = "b";
    }
}
