package forklift.integration.server;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * Created by afrieze on 3/7/17.
 */
public class SchemaRegistryService implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryService.class);

    private Server server;

    private final int localZookeeperPort;
    private final int listenPort;

    public SchemaRegistryService(int localZookeeperPort, int listenPort) {
        this.localZookeeperPort = localZookeeperPort;
        this.listenPort = listenPort;
    }

    public void stop() throws Exception {
        server.stop();
        Thread.sleep(1500);
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty("listeners", "http://localhost:" + listenPort);
        properties.setProperty("kafkastore.connection.url", "localhost:" + localZookeeperPort);
        properties.setProperty("host.name", "localhost");
        //properties.setProperty("kafkastore.topic", "_schemas");
        //properties.setProperty("debug", "false");
        try {
            SchemaRegistryConfig config = new SchemaRegistryConfig(properties);
            SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);
            server = app.createServer();
            server.start();
            server.join();
        } catch (Exception e) {
            log.error("Unable to start Schema Registry", e);
        }
    }
}

