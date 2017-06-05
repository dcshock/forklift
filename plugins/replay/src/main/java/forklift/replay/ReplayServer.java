package forklift.replay;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.consumer.ConsumerService;
import forklift.consumer.ConsumerThread;
import forklift.exception.StartupException;

/**
 * Creates a server using an embedded forklift instance that processes replay messages
 * and writes them to elasticsearch.
 */
public class ReplayServer {
    private final Forklift forklift;
    private final ConsumerThread thread;
    private final Consumer consumer;
    private final ReplayESWriter writer;
    private final boolean standalone;

    private boolean running = false;

    /**
     * Creates a standalone replay server that uses the given connector to read messages and the
     * given connection info to talk to elasticsearch over transport.
     *
     * @param connector the connector to read messages off of
     * @param hostname the hostname to use to talk to elasticsearch
     * @param port the tranposrt port to communicate to elasticserach over
     * @param clusterName the name of the elasticsearch cluster
     */
    public ReplayServer(ForkliftConnectorI connector, String hostname, int port, String clusterName) {
        this(connector, hostname, port, clusterName, true);
    }

    /**
     * Creates a new replay server that uses the given connector to read messages and the given
     * connection info to talk to elasticsearch over transport.
     *
     * @param connector the connector to read messages off of
     * @param hostname the hostname to use to talk to elasticsearch
     * @param port the tranposrt port to communicate to elasticserach over
     * @param clusterName the name of the elasticsearch cluster
     * @param standalone whether this server is embedded or standalone (and thus needs to start and
     *     stop the connector at appropriate times)
     */
    public ReplayServer(ForkliftConnectorI connector, String hostname, int port, String clusterName, boolean standalone) {
        this.forklift = new Forklift();
        this.forklift.setConnector(connector);
        this.standalone = standalone;

        this.writer = new ReplayESWriter(hostname, port, clusterName);

        this.consumer = new Consumer(ReplayConsumer.class, forklift, Thread.currentThread().getContextClassLoader());
        this.consumer.addServices(new ConsumerService(new ReplayConsumerService(this.writer)));
        this.thread = new ConsumerThread(this.consumer);
        this.thread.setName("ReplayES");
        this.thread.setDaemon(true);
    }

    /**
     * Gives the replay writer instance used by this server.
     *
     * @return the replay writer used to write replays to elasticsearch
     */
    public ReplayESWriter getReplayWriter() {
        return writer;
    }

    /**
     * Starts this replay server, including the writer thread, consumer thread,
     * and connector (if it's a standalone server).
     *
     * @throws StartupException if there is an error starting the connector
     */
    public synchronized void start() throws StartupException {
        if (standalone) {
            this.forklift.start(forklift.getConnector());
        }
        this.thread.start();

        running = true;
    }

    /**
     * Starts an asynchronous shutdown of this server, calling the given callback
     * when the shutdown is complete.
     *
     * @param shutdownCallback the callback to call when the shutdown is finished
     */
    public void requestShutdown(ShutdownCallback shutdownCallback) {
        final Thread shutdownThread = new Thread(() -> {
            shutdown();
            shutdownCallback.onShutdown();
        });

        shutdownThread.start();
    }

    /**
     * Stops all of the threads mangaed by this replay server.
     */
    public synchronized void shutdown() {
        if (running) {
            this.thread.shutdown();
            try {
                this.thread.join(180 * 1000);
            } catch (InterruptedException ignored) {
            }

            if (standalone) {
                forklift.shutdown();
            }
            this.writer.close();

            running = false;
        }
    }

    /**
     * Used as a callback for calls to {@link ReplayServer#requestShutdown}
     */
    @FunctionalInterface
    public interface ShutdownCallback {
        /**
         * Called when the {@link ReplayServer} stops due to a call to
         * {@link ReplayServer#requestShutdown(ShutdownCallback)}.
         *
         */
        void onShutdown();
    }
}
