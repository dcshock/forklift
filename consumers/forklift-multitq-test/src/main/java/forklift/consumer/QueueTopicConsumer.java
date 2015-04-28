package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Topic;

@Queue("test1")
@Topic("topic1")
public class QueueTopicConsumer {
    @OnMessage
    public void handle() {
    }
}
