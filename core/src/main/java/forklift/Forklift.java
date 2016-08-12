package forklift;

import forklift.cluster.HeartbeatThread;
import forklift.producers.ForkliftProducerI;
import forklift.consumer.ConsumerThread;
import forklift.decorators.Topic;
import forklift.cluster.Coordinator;
import forklift.consumer.Consumer;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.deployment.DeploymentWatch;
import forklift.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main ForkLift application instance. ForkLift is started here
 * and stopped here.
 */
public class Forklift {
    private static Logger log = LoggerFactory.getLogger("ForkLift");

    private ForkliftConnectorI connector;
    private ForkliftProducerI coordinatorProducer;
    private AtomicBoolean running = new AtomicBoolean(false);
    private ConsumerThread coordinatorThread;
    private HeartbeatThread heartbeatThread;

    public Forklift() {
        log.debug("Creating ForkLift");
    }

    public synchronized void start(ForkliftConnectorI connector)
      throws StartupException {
        this.connector = connector;
        try {
            this.connector.start();
        } catch (ConnectorException e) {
            throw new StartupException(e.getMessage());
        }

        final Consumer coordinator = new Consumer(
            Coordinator.class,
            this.connector,
            Thread.currentThread().getContextClassLoader(),
            Coordinator.class.getAnnotation(Topic.class));

        coordinatorThread = new ConsumerThread(coordinator);
        coordinatorThread.setName("forklift.coordinator");
        coordinatorThread.start();

        this.coordinatorProducer = this.connector.getTopicProducer("forklift.coordinator");

        this.heartbeatThread = new HeartbeatThread(UUID.randomUUID().toString(), this.coordinatorProducer);
        this.heartbeatThread.start();

        running.set(true);
        log.debug("Init complete!");
    }

    public void shutdown() {
        if (!running.getAndSet(false))
            return;

        try {
            this.connector.stop();
        } catch (ConnectorException e) {
            e.printStackTrace();
        }
    }

    public ForkliftConnectorI getConnector() {
        return connector;
    }

    public void setConnector(ForkliftConnectorI connector) {
        this.connector = connector;
    }

    public boolean isRunning() {
        return running.get();
    }

    public static void main(String args[])
      throws StartupException {
        final Forklift forklift = new Forklift();
        final DeploymentWatch deployWatch = new DeploymentWatch(new File("/tmp"), new ConsumerDeploymentEvents(forklift));
        // TODO - get connector from a config file.

        if (!forklift.isRunning())
            throw new RuntimeException("Unable to start Forklift.");
    }
}
