package forklift.replay;

import forklift.decorators.Message;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Order;
import forklift.decorators.Queue;

import javax.inject.Inject;

@Queue("forklift.replay.es?consumer.exclusive=true")
@MultiThreaded(10)
public class ReplayConsumer {
    @Inject private ReplayESWriter writer;
    @Message private ReplayESWriterMsg msg;

    @OnValidate
    public boolean onValidate() {
        return this.writer != null && this.msg != null;
    }

    @OnMessage
    public void onMessage() {
        this.writer.poll(msg);
    }

    @Order
    public String orderBy() {
        return msg.getId();
    }
}
