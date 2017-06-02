package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.source.decorators.Queue;

@Queue("test1")
public class MultiQConsumer {
    @OnMessage
    public void handle() {
    }
}
