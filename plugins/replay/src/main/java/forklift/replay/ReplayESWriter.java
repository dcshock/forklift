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

public class ReplayESWriter extends ReplayStoreThread<ReplayESWriterMsg> {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final TransportClient client;

    public ReplayESWriter(String hostname) {
        this(hostname, 9300, "elasticsearch");
    }

    public ReplayESWriter(String hostname, int port, String clusterName) {
        final Settings settings = Settings.settingsBuilder()
                        .put("cluster.name", clusterName).build();

        this.client = TransportClient.builder()
            .settings(settings)
            .build()
            .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(hostname, port)));
    }

    @Override
    protected void poll(ReplayESWriterMsg t) {
        final String replayVersion = t.getFields().get("forklift-replay-version");
        if ("3".equals(replayVersion)) { // latest version
            String indexDate = t.getFields().get("first-processed-date");
            if (indexDate == null)
                indexDate = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
            final String index = "forklift-replay-" + indexDate;

            try {
                // Index the new information.
                client.prepareIndex(index, "log")
                    .setVersion(t.getVersion()).setVersionType(VersionType.EXTERNAL_GTE)
                    .setId(t.getId()).setSource(t.getFields()).execute().actionGet();
            } catch (VersionConflictEngineException expected) {
                log.debug("Newer replay message already exists", expected);
            }
        } else if ("2".equals(replayVersion) || replayVersion == null) { // older versions didn't have the replay version or didn't persist it with the message
            final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

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
                log.error("", e);
                log.error("Unable to search for old replay logs {}", t.getId());
            }

            try {
                // Index the new information.
                client.prepareIndex(index, "log")
                    .setVersion(t.getVersion()).setVersionType(VersionType.EXTERNAL_GTE)
                    .setId(t.getId()).setSource(t.getFields()).execute().actionGet();
            } catch (VersionConflictEngineException expected) {
                log.debug("Newer replay message already exists", expected);
            }
        } else {
            log.error("Unrecognized replay version: '{}' for message ID '{}' with fields '{}' ", replayVersion, t.getId(), t.getFields());
        }
    }

    public void shutdown() {
        if (client != null)
            client.close();
    }
}
