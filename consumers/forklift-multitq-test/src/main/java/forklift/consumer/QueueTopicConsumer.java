package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.source.decorators.Queue;
import forklift.source.decorators.Topic;

@Queue("test1")
@Topic("topic1")
public class QueueTopicConsumer {
    @OnMessage
    public void handle() {
    }
}
