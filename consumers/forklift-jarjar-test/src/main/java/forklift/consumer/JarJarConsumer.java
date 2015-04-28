package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.decorators.Queue;

@Queue("test")
public class JarJarConsumer {
    @OnMessage
    public void handle() {
    }
}
