package forklift.integration.kafka.Consumer;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Topic;

/**
 * Created by afrieze on 3/2/17.
 */
@Topic("forklift-pojoTopic")
public class TestPersonConsumer {
    private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestStringConsumer.class);

    @Message
    private Person person;

    @OnMessage
    public void consume() {
        log.info("Consumer for queue forklift-personTopic consumed value: " + person.getFirstName());
    }
}
