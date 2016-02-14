package forklift;

import static org.kohsuke.args4j.ExampleMode.ALL;

import consul.Consul;
import forklift.connectors.ActiveMQConnector;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.consumer.LifeCycleMonitors;
import forklift.deployment.DeploymentWatch;
import forklift.replay.ReplayES;
import forklift.replay.ReplayLogger;
import forklift.retry.RetryES;
import forklift.retry.RetryHandler;
import org.apache.activemq.broker.BrokerService;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
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

    private static BrokerService broker = null;

    /**
     * Launch a Forklift server instance.
     */
    public static void main(String[] args) throws Throwable {
        final ForkliftOpts opts = new ForkliftOpts();
        final CmdLineParser argParse = new CmdLineParser(opts);
        try {
            argParse.parseArgument(args);
        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java SampleMain [options...] arguments...");
            // print the list of available options
            argParse.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java SampleMain" + argParse.printExample(ALL));

            return;
        }

        File f = new File(opts.getConsumerDir());
        if (!f.exists() || !f.isDirectory()) {
            System.err.println();
            System.err.println(" -monitor1 is not a valid directory.");
            System.err.println();
            argParse.printUsage(System.err);
            System.err.println();
            return;
        }


        String brokerUrl = opts.getBrokerUrl();
        if (brokerUrl.startsWith("consul.") && brokerUrl.length() > "consul.".length()) {
            log.info("Building failover url using consul");

            final Consul c = new Consul("http://" + opts.getConsulHost(), 8500);

            // Build the connection string.
            final String serviceName = brokerUrl.split("\\.")[1];

            brokerUrl = "failover:(" +
                c.catalog().service(serviceName).getProviders().stream()
                    .filter(srvc -> !srvc.isCritical())
                    .map(srvc -> "tcp://" + srvc.getAddress() + ":" + srvc.getPort())
                    .reduce("", (a, b) -> a + "," + b) +
                ")";

            c.shutdown();

            brokerUrl = brokerUrl.replaceAll("failover:\\(,", "failover:(");

            log.info("url {}", brokerUrl);
            if (brokerUrl.equals("failover:()")) {
                log.error("No brokers found");
                System.exit(-1);
            }
        } else if (brokerUrl.startsWith("embed")) {
            brokerUrl = "tcp://0.0.0.0:61616";
            broker = new BrokerService();

            // configure the broker
            broker.addConnector(brokerUrl);
            broker.addConnector("stomp://0.0.0.0:61613");

            broker.start();
        }

        // Start a forklift server w/ specified connector.
        final Forklift forklift = new Forklift();
        final ConsumerDeploymentEvents deploymentEvents = new ConsumerDeploymentEvents(forklift);

        final DeploymentWatch deploymentWatch;
        if (opts.getConsumerDir() != null)
            deploymentWatch = new DeploymentWatch(new java.io.File(opts.getConsumerDir()), deploymentEvents);
        else
            deploymentWatch = null;

        final DeploymentWatch propsWatch;
        if (opts.getPropsDir() != null)
            propsWatch = new DeploymentWatch(new java.io.File(opts.getPropsDir()), deploymentEvents);
        else
            propsWatch = null;

        forklift.start(new ActiveMQConnector(brokerUrl));
        if (!forklift.isRunning()) {
            throw new RuntimeException("Unable to start Forklift");
        }

        log.info("Registering LifeCycleMonitors");

        // Create the replay ES first if it's needed just in case we are utilizing the startup of the embedded es engine.
        ReplayES replayES = null;
        if (opts.getReplayESHost() != null)
             replayES = new ReplayES(!opts.isReplayESServer(), opts.isReplayESSsl(), opts.getReplayESHost(), opts.getReplayESPort());

        // Setup retry handling.
        if (opts.getRetryDir() != null)
            LifeCycleMonitors.register(new RetryHandler(forklift.getConnector(), new File(opts.getRetryDir())));
        if (opts.getRetryESHost() != null)
            LifeCycleMonitors.register(new RetryES(forklift.getConnector(), opts.isRetryESSsl(), opts.getRetryESHost(), opts.getRetryESPort(), opts.isRunRetries()));

        // Always add replay last so that other plugins can update props.
        if (replayES != null)
            LifeCycleMonitors.register(replayES);
        if (opts.getReplayDir() != null)
            LifeCycleMonitors.register(new ReplayLogger(new File(opts.getReplayDir())));

        log.info("Connected to broker on " + brokerUrl);
        log.info("Scanning for Forklift consumers at " + opts.getConsumerDir());
        log.info("Scanning for Forklift consumers at " + opts.getPropsDir());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // End the deployment watcher.
                running.set(false);
                synchronized (running) {
                    running.notify();
                }

                if (deploymentWatch != null)
                    deploymentWatch.shutdown();

                if (propsWatch != null)
                    propsWatch.shutdown();

                forklift.shutdown();

                if (broker != null)
                    try {
                        broker.stop();
                    } catch (Exception ignored) { }
            }
        });

        running.set(true);
        while (running.get()) {
            log.debug("Scanning for new deployments...");

            try {
                if (deploymentWatch != null)
                    deploymentWatch.run();
            } catch (Throwable e) {
                log.error("", e);
            }

            try {
                if (propsWatch != null)
                    propsWatch.run();
            } catch (Throwable e) {
                log.error("", e);
            }

            synchronized (running) {
                running.wait(SLEEP_INTERVAL);
            }
        }
    }
}
