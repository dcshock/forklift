package forklift.replay;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplayESWriter {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private String hostname;
    private int port;
    private String clusterName;

    private TransportClient client;
    private RecoveryCallback recoveryCallback;

    private ReadWriteLock clientLock = new ReentrantReadWriteLock();

    public ReplayESWriter(String hostname) {
        this(hostname, 9300, "elasticsearch");
    }

    public ReplayESWriter(String hostname, int port, String clusterName) {
        recreateEsTransportClient(hostname, port, clusterName);
    }

    public void recreateEsTransportClient() {
        if (client != null) {
            client.close();
        }

        final Settings settings = Settings.settingsBuilder()
            .put("cluster.name", clusterName).build();

        clientLock.writeLock().lock();
        this.client = TransportClient.builder()
            .settings(settings)
            .build()
            .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(hostname, port)));
        clientLock.writeLock().unlock();
    }

    public void recreateEsTransportClient(String hostname, int port, String clusterName) {
        this.hostname = hostname;
        this.port = port;
        this.clusterName = clusterName;

        recreateEsTransportClient();
    }

    public void setRecoveryCallback(RecoveryCallback callback) {
        this.recoveryCallback = callback;
    }

    public RecoveryCallback getRecoveryCallback() {
        return recoveryCallback;
    }

    protected void poll(ReplayESWriterMsg t) {
        final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        clientLock.readLock().lock();
        // In order to ensure there is only one replay msg for a given id we have to clean the msg id from
        // any previously created indexes.
        try {
            final SearchResponse resp = client.prepareSearch("forklift-replay*").setTypes("log")
                .setQuery(QueryBuilders.termQuery("_id", t.getId()))
                .setFrom(0).setSize(50).setExplain(false)
                .execute()
                .actionGet();

            if (resp != null && resp.getHits() != null && resp.getHits().getHits() != null) {
                for (SearchHit hit : resp.getHits().getHits()) {
                    if (!hit.getIndex().equals(index))
                        client.prepareDelete(hit.getIndex(), "log", t.getId()).execute().actionGet();
                }
            }
        } catch (Exception e) {
            log.error("Unable to search for old replay logs {}", t.getId(), e);
            recoveryCallback.onError(e);
        }

        try {
            // Index the new information.
            client.prepareIndex(index, "log")
                .setVersion(t.getVersion()).setVersionType(VersionType.EXTERNAL_GTE)
                .setId(t.getId()).setSource(t.getFields()).execute().actionGet();
        } catch (VersionConflictEngineException expected) {
            log.debug("Newer replay message already exists", expected);
        }
        clientLock.readLock().unlock();
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Used to control how errors are handled by this replay writer
     */
    @FunctionalInterface
    public interface RecoveryCallback {
        /**
         * Called when an error occurs in the started {@link ReplayConsumer}.
         *
         * @param t the error that occurred
         */
        void onError(Throwable t);
    }
}
