package forklift.integration.server;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by afrieze on 3/7/17.
 */
public class KafkaService implements Runnable {

    private KafkaServerStartable kafka;

    public File dataDir = new File("src/test/resources/zafka");
    private final int localZookeeperPort;
    private final int listenPort;

    public KafkaService(int localZookeeperPort, int listenPort) {
        this.localZookeeperPort = localZookeeperPort;
        this.listenPort = listenPort;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty("broker.id", "1");
        properties.setProperty("listeners", "PLAINTEXT://:" + listenPort);
        properties.setProperty("num.network.threads", "3");
        properties.setProperty("num.io.threads", "8");
        properties.setProperty("socket.send.buffer.bytes", "102400");
        properties.setProperty("socket.receive.buffer.bytes", "102400");
        properties.setProperty("socket.request.max.bytes", "104857600");
        properties.setProperty("log.dirs", dataDir.getAbsolutePath());
        properties.setProperty("num.partitions", "16");
        properties.setProperty("num.recovery.threads.per.data.dir", "1");
        properties.setProperty("log.retention.hours", "168");
        properties.setProperty("log.segment.bytes", "1073741824");
        properties.setProperty("log.retention.check.interval.ms", "300000");
        properties.setProperty("zookeeper.connect", "localhost:" + localZookeeperPort);
        properties.setProperty("zookeeper.connection.timeout.ms", "6000");
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();
    }

    public void stop() throws InterruptedException, IOException {
        kafka.shutdown();
        Thread.sleep(1500);
    }

    public File getDataDirectoryFile() {
        return dataDir;
    }
}
