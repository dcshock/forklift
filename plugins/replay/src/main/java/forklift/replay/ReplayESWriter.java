package forklift.replay;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ReplayESWriter extends ReplayStoreThread<ReplayESWriterMsg> {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final RestClient restClient;

    public ReplayESWriter(String hostname) {
        this(hostname, 9200, "elasticsearch");
    }

    // clusterName is retained in constructor for compatibility
    public ReplayESWriter(String hostname, int port, String clusterName) {
        this.restClient = RestClient.builder(new HttpHost(hostname, port, "http"))
            .setRequestConfigCallback(requestConfig -> requestConfig
                                                           .setConnectTimeout(3_000)
                                                           .setSocketTimeout(20_000))
            .setDefaultHeaders(new Header[] {
                 new BasicHeader("Accept", "application/json; charset=utf-8"),
                 new BasicHeader("Content-Type", "application/json; charset=utf-8")
             })
            .setMaxRetryTimeoutMillis(20_000)
            .build();
    }

    @Override
    protected void poll(ReplayESWriterMsg replayMessage) {
        final String replayVersion = replayMessage.getFields().get("forklift-replay-version");
        if ("3".equals(replayVersion)) { // latest version
            processVersion3Replay(replayMessage);
        } else if ("2".equals(replayVersion) || replayVersion == null) { // older versions didn't have the replay version or didn't persist it with the message
            processVersion2OrEarlierReplay(replayMessage);
        } else {
            log.error("Unrecognized replay version: '{}' for message ID '{}' with fields '{}'",
                      replayVersion, replayMessage.getId(), replayMessage.getFields());
        }
    }

    private void processVersion3Replay(final ReplayESWriterMsg replayMessage) {
        String indexDate = replayMessage.getFields().get("first-processed-date");
        if (indexDate == null) {
            indexDate = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        }
        final String index = "forklift-replay-" + indexDate;

        indexReplayMessage(replayMessage, index);
    }

    private void processVersion2OrEarlierReplay(ReplayESWriterMsg replayMessage) {
        final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        for (String existingMessageIndex : searchForIndexesWithId(replayMessage.getId())) {
            if (!existingMessageIndex.equals(index)) {
                deleteMessageInIndex(replayMessage.getId(), existingMessageIndex);
            }
        }

        indexReplayMessage(replayMessage, index);
    }

    private void indexReplayMessage(final ReplayESWriterMsg replayMessage, final String index) {
        final String id = replayMessage.getId();
        final String endpoint =  "/" + index + "/log/" +  id;

        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("version_type", "external_gte");
        queryParams.put("version", "" + replayMessage.getVersion());

        final HttpEntity entity;
        try {
            final String entityContents = mapper.writeValueAsString(replayMessage.getFields());
            entity = new NStringEntity(entityContents, ContentType.APPLICATION_JSON);
        } catch (JsonProcessingException e) {
            log.error("Could not write replay fields to JSON: (id {}, fields {})", replayMessage.getId(), replayMessage.getFields(), e);
            return;
        }

        try {
            restClient.performRequest("PUT", endpoint, queryParams, entity);
        } catch (ResponseException e) {
            // conflicts are normal when versioned index requests are submitted in the wrong order
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT) {
                return;
            }

            log.error("Error indexing replay message (id {}, fields {})", id, replayMessage.getFields(), e);
        } catch (IOException e) {
            log.error("Error indexing replay message (id {}, fields {})", id, replayMessage.getFields(), e);
        }
    }

    private Iterable<String> searchForIndexesWithId(final String id) {
        final String endpoint = "/forklift-replay*/_search";
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("q", "_id:" + id);
        queryParams.put("size", "50");

        try {
            final Response response = restClient.performRequest("GET", endpoint, queryParams);
            try {
                final JsonNode jsonResponse = mapper.readTree(response.getEntity().getContent());
                final JsonNode hits = jsonResponse.get("hits").get("hits");
                final List<String> indexes = new ArrayList<>();

                for (final JsonNode hit : hits) {
                    final String hitIndex = hit.get("_index").asText();
                    indexes.add(hitIndex);
                }

                return indexes;
            } catch (Exception e) {
                log.error("Error parsing elasticsearch response to search for id {}", id, e);
            }
        } catch (IOException e) {
            log.error("Error searching for indexes for id {}", id,  e);
        }

        return Collections.emptyList();
    }

    private void deleteMessageInIndex(final String id, final String index) {
        final String endpoint = "/" + index + "/log/" + id;
        try {
            restClient.performRequest("DELETE", endpoint);
        } catch (IOException e) {
            log.error("Error deleting message with id {} for index {}", id, index,  e);
        }
    }

    public void shutdown() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                log.error("Couldn't shutdown Elasticsearch REST client", e);
            }
        }
    }
}
