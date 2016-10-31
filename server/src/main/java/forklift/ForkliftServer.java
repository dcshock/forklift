package forklift;

import consul.Consul;
import forklift.connectors.ActiveMQConnector;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.ConsumerDeploymentEvents;
import forklift.consumer.LifeCycleMonitors;
import forklift.deployment.Deployment;
import forklift.deployment.DeploymentManager;
import forklift.deployment.DeploymentWatch;
import forklift.deployment.ClassDeployment;
import forklift.replay.ReplayES;
import forklift.replay.ReplayLogger;
import forklift.retry.RetryES;
import forklift.retry.RetryHandler;
import forklift.stats.StatsCollector;
import org.apache.activemq.broker.BrokerService;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.kohsuke.args4j.ExampleMode.ALL;

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

    private final ForkliftOpts opts;

    public ForkliftServer(ForkliftOpts options){
        this.opts = options;
    }

    private ConsumerDeploymentEvents deploymentEvents;

    private DeploymentManager classDeployments = new DeploymentManager();

    /**
     * Launch a Forklift server instance.
     */
    public static void main(String[] args) throws Throwable {
        final ForkliftOpts opts = new ForkliftOpts();
        final CmdLineParser argParse = new CmdLineParser(opts);
        try {
            argParse.parseArgument(args);
        } catch (CmdLineException e) {
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
        ForkliftServer server = new ForkliftServer(opts);
        server.launch();
    }

    /**
     * Launch a Forklift server instance.
     */
    public void launch() throws Throwable {
        String brokerUrl = startBroker();
        final Forklift forklift = new Forklift();
        deploymentEvents = new ConsumerDeploymentEvents(forklift);
        DeploymentWatch consumerWatch = setupConsumerWatch(deploymentEvents, forklift);
        DeploymentWatch propsWatch = setupPropertyWatch(deploymentEvents);
        final ForkliftConnectorI connector = new ActiveMQConnector(brokerUrl);
        forklift.start(connector);
        if (!forklift.isRunning()) {
            throw new RuntimeException("Unable to start Forklift");
        }
        ReplayES replayES = setupESReplayHandling(forklift);
        RetryES retryES = setupESRetryHandling(forklift);
        setupLifeCycleMonitors(replayES, retryES, forklift);
        setupShutdownHook(replayES, consumerWatch, forklift);
        runEventLoop(propsWatch, consumerWatch);
    }

    /**
     *
     * @param deploymentClasses the classes which make up the deployment
     */
    public synchronized void registerDeployment(Class<?> ...deploymentClasses){
        if(!running.get()) {
            throw new IllegalStateException("Forklift Server not running!");
        }
        Deployment deployment = new ClassDeployment(deploymentClasses);
        if (!classDeployments.isRegistered(deployment)) {
            classDeployments.register(deployment);
            deploymentEvents.onDeploy(deployment);
        }

    }

    private void runEventLoop(DeploymentWatch propsWatch, DeploymentWatch consumerWatch) throws InterruptedException {
        running.set(true);
        while (running.get()) {
            log.debug("Scanning for new deployments...");

            try {
                if (propsWatch != null)
                    propsWatch.run();
            } catch (Throwable e) {
                log.error("", e);
            }

            try {
                if (consumerWatch != null)
                    consumerWatch.run();
            } catch (Throwable e) {
                log.error("", e);
            }

            synchronized (running) {
                running.wait(SLEEP_INTERVAL);
            }
        }
    }

    private void setupShutdownHook(ReplayES replayES, DeploymentWatch consumerWatch, Forklift forklift) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

                // End the deployment watcher.
                running.set(false);

                synchronized (running) {
                    running.notify();
                }

                if(replayES != null) {
                    replayES.shutdown();
                }

                if (consumerWatch != null)
                    consumerWatch.shutdown();

                classDeployments.getAll().stream().forEach(deploy -> deploymentEvents.onUndeploy(deploy));

                if (consumerWatch != null)
                    consumerWatch.shutdown();

                forklift.shutdown();

                if (broker != null) {
                    try {
                        broker.stop();
                    } catch (Exception ignored) {
                    }
                }
            }
        });
    }

    private void setupLifeCycleMonitors(ReplayES replayES, RetryES retryES, Forklift forklift) throws FileNotFoundException {
        LifeCycleMonitors.register(StatsCollector.class);

        // Setup retry handling.
        if(retryES != null){
            LifeCycleMonitors.register(retryES);
        }
        if(opts.getRetryDir() != null){
            LifeCycleMonitors.register(new RetryHandler(forklift.getConnector(), new File(opts.getRetryDir())));
        }
        // Always add replay last so that other plugins can update props.
        if (replayES != null)
            LifeCycleMonitors.register(replayES);
        if (opts.getReplayDir() != null)
            LifeCycleMonitors.register(new ReplayLogger(new File(opts.getReplayDir())));
    }

    private ReplayES setupESReplayHandling(Forklift forklift) {
        // Create the replay ES first if it's needed just in case we are utilizing the startup of the embedded es engine.
        final ReplayES replayES;
        if (opts.getReplayESHost() == null)
            replayES = null;
        else
            replayES = new ReplayES(!opts.isReplayESServer(), opts.isReplayESSsl(), opts.getReplayESHost(), opts.getReplayESPort(), forklift.getConnector());
        return replayES;
    }

    private RetryES setupESRetryHandling(Forklift forklift) {
        RetryES retryES = null;
        if (opts.getRetryESHost() != null)
            retryES = new RetryES(forklift.getConnector(), opts.isRetryESSsl(), opts.getRetryESHost(), opts.getRetryESPort(), opts.isRunRetries());
        return retryES;
    }

    private DeploymentWatch setupConsumerWatch(ConsumerDeploymentEvents deploymentEvents, Forklift forklift) {
        final DeploymentWatch deploymentWatch;
        if (opts.getConsumerDir() != null) {
            deploymentWatch = new DeploymentWatch(new java.io.File(opts.getConsumerDir()), deploymentEvents);
            log.info("Scanning for Forklift consumers at " + opts.getConsumerDir());
        } else {
            deploymentWatch = null;
        }
        return deploymentWatch;
    }

    private DeploymentWatch setupPropertyWatch(ConsumerDeploymentEvents deploymentEvents) {
        final DeploymentWatch propsWatch;
        if (opts.getPropsDir() != null) {
            propsWatch = new DeploymentWatch(new java.io.File(opts.getPropsDir()), deploymentEvents);
            log.info("Scanning for Properties at " + opts.getPropsDir());
        }
        else {
            propsWatch = null;
        }
        return propsWatch;
    }

    private String startBroker() throws Exception {
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
        log.info("Connected to broker on " + brokerUrl);
        return brokerUrl;
    }
}
