package forklift.integration.kafka;

import static org.junit.Assert.assertTrue;
import com.mashape.unirest.http.options.Options;
import forklift.ForkliftOpts;
import forklift.ForkliftServer;
import forklift.ServerState;
import forklift.integration.kafka.Consumer.TestMapConsumer;
import forklift.integration.kafka.Consumer.TestPersonConsumer;
import forklift.integration.kafka.Consumer.TestStringConsumer;
import org.junit.Ignore;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

/**
 * Kafka integration test.  Tests the embeddable server with a live kafka and confluent schema registry instance. These
 * tests pair well with the integration tests in the Kafka connector project.
 * <p>
 * Note: You will need to adjust the broker url to match your own environment.
 */
public class ServerIntegrationTests {

    @Ignore
    @Test
    public void testConsumers() throws InterruptedException {
        ForkliftOpts options = new ForkliftOpts();
        String ip = "localhost";
        options.setBrokerUrl("consul.kafka.schema-registry");
        options.setApplicationName("app10");
        options.setConsulHost(ip);
        ForkliftServer server = new ForkliftServer(options);
        try {
            server.startServer(10000, TimeUnit.SECONDS);
            // Forklift closes Unirest...we need to reopen it with a call to Options.refresh()
            Options.refresh();
        } catch (InterruptedException e) {
        }
        if (server.getServerState() == ServerState.RUNNING) {
            server.registerDeployment(TestStringConsumer.class, TestMapConsumer.class, TestPersonConsumer.class);
        } else {
            try {
                server.stopServer(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            System.exit(-1);
        }
        Thread.sleep(130000);
        server.stopServer(5, TimeUnit.SECONDS);
        assertTrue(true);
    }
}
