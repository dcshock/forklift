package forklift.integration.kafka.Consumer;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Topic;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by afrieze on 3/2/17.
 */
@Topic("forklift-mapTopic")
public class TestMapConsumer {
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestStringConsumer.class);

    @Message
    private Map<String,String> message;

    @OnMessage
    public void consume() {
        String mapValues = message.keySet().stream().map(x->x + "=" + message.get(x)).collect(Collectors.joining(","));
        log.info("Consumer for queue forklift-objectTopic consumed map with values: " + mapValues);
    }
}
