package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.decorators.Queue;

@Queue("test")
public class TestQueueConsumer {
    @OnMessage
    public void handle() {
    }
}
