package forklift.integration.kafka.Consumer;

import forklift.decorators.Message;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Topic;

/**
 * Created by afrieze on 3/1/17.
 */
@MultiThreaded(10)
@Topic("forklift-stringTopic")
public class TestStringConsumer {
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestStringConsumer.class);

    @Message
    private String value;

    @OnMessage
    public void consume() {
        log.info("Consumer for queue forklift-stringTopic consumed value: " + value);
    }
}
