package forklift;

import forklift.Forklift;
import forklift.connectors.ActiveMQConnector;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.deployment.DeploymentWatch;

/**
 * Start Forklift as a server.
 * @author zdavep
 */
public final class ForkliftServer {

    // Logging
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForkliftServer.class);

    // Consumer deployment interval
    private static int SLEEP_INTERVAL = 10000; // 10 seconds

    /**
     * Launch a Forklift server instance.
     */
    public static void main(String[] args) throws Throwable {

        // Read CLI flags (if provided).
        final String brokerUrl  = (args.length >= 1) ? args[0] : "tcp://127.0.0.1:61616";
        final String scanDir    = (args.length >= 2) ? args[1] : "/usr/local/forklift/consumers";

        // TODO: Read connector from CLI or config file here...

        // Start a forklift server w/ specified connector.
        final Forklift forklift = new Forklift();
        final ConsumerDeploymentEvents deploymentEvents = new ConsumerDeploymentEvents(forklift);
        final DeploymentWatch deploymentWatch = new DeploymentWatch(new java.io.File(scanDir), deploymentEvents);
        forklift.start(new ActiveMQConnector(brokerUrl));
        if (!forklift.isRunning()) {
            throw new RuntimeException("Unable to start Forklift");
        }
        log.info("Connected to broker on " + brokerUrl);
        log.info("Scanning for Forklift consumers at " + scanDir);

        // Poll for new deployments
        while (forklift.isRunning()) {
            log.debug("Scanning for new deployments...");
            deploymentWatch.run();
            Thread.sleep(SLEEP_INTERVAL);
        }
    }

}
