package forklift.integration.server;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by afrieze on 3/7/17.
 */
public class ZookeeperService implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperService.class);
    private ZookeeperServer server;
    private File dataDir = new File("src/test/resources/zookeeper");
    Thread runningThread;
    private int clientPort;

    public ZookeeperService(int clientPort) {
        this.clientPort = clientPort;
    }

    @Override
    public void run() {
        runningThread = Thread.currentThread();
        Properties properties = new Properties();
        properties.setProperty("tickTime", "2000");
        properties.setProperty("initLimit", "10");
        properties.setProperty("dataDir", dataDir.getAbsolutePath());
        properties.setProperty("clientPort", Integer.toString(clientPort));
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
            final ServerConfig configuration = new ServerConfig();
            configuration.readFrom(quorumConfiguration);
            server = new ZookeeperServer();
            server.runFromConfig(configuration);
        } catch (Exception e) {
            log.error("Unable to start embedded zookeeper server", e);
        }

    }

    public void stop() throws InterruptedException, IOException {
        server.shutdown();
        runningThread.interrupt();
        Thread.sleep(1500);
    }

    public File getDataDirectoryFile() {
        return dataDir;
    }

    private class ZookeeperServer extends ZooKeeperServerMain{
        @Override
        public void shutdown(){
            super.shutdown();
        }
    }
}
