package forklift;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.deployment.DeploymentWatch;
import forklift.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main ForkLift application instance. ForkLift is started here
 * and stopped here.
 */
public class Forklift {
    private static Logger log = LoggerFactory.getLogger("ForkLift");

    private ForkliftConnectorI connector;
    private AtomicBoolean running = new AtomicBoolean(false);

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
