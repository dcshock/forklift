package forklift.replay;

import forklift.replay.ReplayESWriter.ReplayESWriterMsg;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class ReplayESWriter extends ReplayStoreThread<ReplayESWriterMsg> {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final JestClient client;

    public static class ReplayESWriterMsg {
        private String id;
        private Map<String, String> fields;

        public ReplayESWriterMsg(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = fields;
        }
    }

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

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (client != null)
                    client.shutdownClient();
            }
        });
    }

    @Override
    protected void poll(ReplayESWriterMsg t) {
        final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        try {
            client.execute(new Delete.Builder(t.id).index("forklift-replay*").type("msg").build());
            client.execute(new Index.Builder(t.fields).index(index).type("log").id(t.id).build());
        } catch (IOException e) {
            log.error("Unable to index replay log", e);
        }
    }
}
