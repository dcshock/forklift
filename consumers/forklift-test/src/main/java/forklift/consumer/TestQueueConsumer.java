package forklift.consumer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.source.decorators.Queue;

@Queue("test")
public class TestQueueConsumer {
    Logger log = LoggerFactory.getLogger(TestQueueConsumer.class);

    @Message
    public Map<String, String> msg;

    @OnMessage
    public void handle() {
        log.info("My message was {}", msg);
    }
}
