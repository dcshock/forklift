package forklift.replay;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Queue;

import javax.inject.Inject;

@Queue("forklift.replay.es?consumer.exclusive=true")
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
}
