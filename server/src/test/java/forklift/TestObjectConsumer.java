package forklift;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Topic;

/**
 * Created by afrieze on 3/2/17.
 */
@Topic("forklift-objectTopic")
public class TestObjectConsumer {
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestStringConsumer.class);

    @Message
    private com.sofi.avro.schemas.Test value;

    @OnMessage
    public void consume() {
        log.info("Consumer for queue forklift-objectTopic consumed value: " + value.getName());
    }
}
