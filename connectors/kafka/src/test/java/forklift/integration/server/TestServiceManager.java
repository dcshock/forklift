package forklift.integration.server;

import forklift.Forklift;
import forklift.connectors.KafkaConnector;
import forklift.exception.StartupException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Contains all of the necessary connection management code that tests
 * need in order to run against the kafka, zookeeper, and schema registry.
 *
 * @author mconroy
 * @author afrieze
 */
public class TestServiceManager {
    private static final Logger log = LoggerFactory.getLogger(TestServiceManager.class);
    private ExecutorService executor = Executors.newFixedThreadPool(3);
    private  ZookeeperService zookeeper;
    private  KafkaService kafka;
    private  SchemaRegistryService schemaRegistry;
    private  KafkaConnector connector;
    private  TelnetClient telnet;
    private  int zookeeperPort = 42181;
    private  int kafkaPort = 49092;
    private  int schemaPort = 58081;
    private  List<Forklift> forklifts = new ArrayList<>();

    public void start() {

        try {
            zookeeper = new ZookeeperService(zookeeperPort);
            kafka = new KafkaService(zookeeperPort, kafkaPort);
            schemaRegistry = new SchemaRegistryService(zookeeperPort, schemaPort);

            this.removeDataDirectories();
            executor.execute(zookeeper);
            waitService("127.0.0.1", zookeeperPort);
            executor.execute(kafka);
            waitService("127.0.0.1", kafkaPort);
            Thread.sleep(2000); //kafka doesn't seem to be quite ready after it starts listening on its port
            executor.execute(schemaRegistry);
            waitService("127.0.0.1", schemaPort);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void waitService(String host, int port) throws InterruptedException {
        telnet = new TelnetClient();
        int triesLeft = 100;
        while (triesLeft > 0)
            try {
                telnet.connect(host, port);
                triesLeft = 0;
                telnet.disconnect();
            } catch (Exception e) {
                Thread.sleep(50);
                triesLeft--;
            }
    }

    public void stop() {

        // Kill the broker and cleanup the testing data.
        try {
            forklifts.forEach(forklift -> forklift.shutdown());
            schemaRegistry.stop();
            kafka.stop();
            zookeeper.stop();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            removeDataDirectories();
        }
    }
    public void removeDataDirectories(){
        try {
            removeDirectory(kafka.getDataDirectoryFile());
            removeDirectory(zookeeper.getDataDirectoryFile());
        } catch (Exception e) {

        }
    }

    private void removeDirectory(File file) throws InterruptedException{
        int retries = 5;
        while(retries > 0){
            try {
                FileUtils.deleteDirectory(file);
                retries = 0;
            } catch (IOException e) {
                retries--;
                Thread.sleep(1000);
            }
        }
    }

    public Forklift newManagedForkliftInstance() throws StartupException {
        return newManagedForkliftInstance("");
    }

    public Forklift newManagedForkliftInstance(String controllerName) throws StartupException {
        // Verify that we can get a kafka connection to the broker.
        String kafkaUri = "127.0.0.1:" + kafkaPort;
        String schemaUrl = "http://127.0.0.1:" + schemaPort;
        connector = new KafkaConnector(kafkaUri, schemaUrl, "testGroup");
        Forklift forklift = new Forklift();
        forklift.start(connector);
        forklifts.add(forklift);
        return forklift;
    }
}
