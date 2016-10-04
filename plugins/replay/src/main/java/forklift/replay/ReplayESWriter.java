package forklift.replay;

import com.google.gson.JsonArray;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class ReplayESWriter extends ReplayStoreThread<ReplayESWriterMsg> {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final JestClient client;

    public ReplayESWriter(boolean ssl, String hostname) {
        this(ssl, hostname, 9200);
    }

    public ReplayESWriter(boolean ssl, String hostname, int port) {
        final String prefix;
        if (ssl)
            prefix = "https://";
        else
            prefix = "http://";

        final JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
            new HttpClientConfig.Builder(prefix + hostname + ":" + port)
               .multiThreaded(true)
               .build());
        client = factory.getObject();
    }

    @Override
    protected void poll(ReplayESWriterMsg t) {
        final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        // In order to ensure there is only one replay msg for a given id we have to clean the msg id from
        // any previously created indexes.
        try {
            final String q = String.format(
                "{\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"query\":\"_id:\\\"%s\\\"\"}}}},\"fields\":[],\"from\":0,\"size\":50,\"explain\":false}", t.getId());
            final Search search = new Search.Builder(q).addIndex("forklift-replay*").build();
            final SearchResult results = client.execute(search);
            if (results != null && results.getTotal() > 0) {
                final JsonArray arr = results.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
                arr.forEach((a) -> {
                    try {
                        client.execute(new Delete.Builder(t.getId()).index(a.getAsJsonObject().get("_index").getAsString()).type("log").build());
                    } catch (Exception e) {
                        log.error("", e);
                    }
                });
            }
        } catch (IOException e) {
            log.error("Unable to search for old replay logs {}", t.getId());
        }

        // Ignore IOExceptions for the first 3 attempts.
        for (int i = 0; i < 3; i++) {
            try {
                // Index the new information.
                client.execute(new Index.Builder(t.getFields()).index(index).type("log").id(t.getId()).build());
                break;
            } catch (IOException e) {
                if (i == 2)
                    log.error("Unable to index replay log: {}", t.getFields().toString(), e);
            }
        }
    }

    public void shutdown() {
        if (client != null)
            client.shutdownClient();
    }
}
