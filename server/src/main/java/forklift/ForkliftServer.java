package forklift;

import consul.Consul;
import forklift.connectors.ActiveMQConnector;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.consumer.LifeCycleMonitors;
import forklift.deployment.DeploymentWatch;
import forklift.replay.ReplayLogger;
import forklift.retry.RetryHandler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Start Forklift as a server.
 * @author zdavep
 */
public final class ForkliftServer {
    // Lock Waits
    private static final AtomicBoolean running = new AtomicBoolean(false);

    // Logging
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ForkliftServer.class);

    // Consumer deployment interval
    private static int SLEEP_INTERVAL = 10000; // 10 seconds

    /**
     * Launch a Forklift server instance.
     */
    public static void main(String[] args) throws Throwable {

        // Read CLI flags (if provided).
        String brokerUrl  = (args.length >= 1) ? args[0] : "tcp://127.0.0.1:61616";
        final String scanDir    = (args.length >= 2) ? args[1] : "/usr/local/forklift/consumers";

        if (brokerUrl.startsWith("consul.") && brokerUrl.length() > "consul.".length()) {
            log.info("Building failover url using consul");

            final Consul c = new Consul("http://dev4", 8500);

            // Build the connection string.
            final String serviceName = brokerUrl.split("\\.")[1];

            brokerUrl = "failover:(" +
                c.catalog().service(serviceName).getProviders().stream()
                    .filter(srvc -> !srvc.isCritical())
                    .map(srvc -> "tcp://" + srvc.getAddress() + ":" + srvc.getPort())
                    .reduce("", (a, b) -> a + "," + b) +
                ")";

            c.shutdown();

            log.info("url {}", brokerUrl);
            if (brokerUrl.equals("failover:()")) {
                log.error("No brokers found");
                System.exit(-1);
            }
        }

        // Start a forklift server w/ specified connector.
        final Forklift forklift = new Forklift();
        final ConsumerDeploymentEvents deploymentEvents = new ConsumerDeploymentEvents(forklift);
        final DeploymentWatch deploymentWatch = new DeploymentWatch(new java.io.File(scanDir), deploymentEvents);
        forklift.start(new ActiveMQConnector(brokerUrl));
        if (!forklift.isRunning()) {
            throw new RuntimeException("Unable to start Forklift");
        }

        log.info("Registering LifeCycleMonitors");
        LifeCycleMonitors.register(new RetryHandler(forklift.getConnector()));
        LifeCycleMonitors.register(ReplayLogger.class);

        log.info("Connected to broker on " + brokerUrl);
        log.info("Scanning for Forklift consumers at " + scanDir);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // End the deployment watcher.
                running.set(false);
                synchronized (running) {
                    running.notify();
                }

                deploymentWatch.shutdown();
                forklift.shutdown();
            }
        });

        running.set(true);
        while (running.get()) {
            log.debug("Scanning for new deployments...");
            deploymentWatch.run();
            synchronized (running) {
                running.wait(SLEEP_INTERVAL);
            }
        }
    }
}
