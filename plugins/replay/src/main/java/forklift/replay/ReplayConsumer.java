package forklift.replay;

import forklift.decorators.Message;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Order;
import forklift.source.decorators.Queue;

import javax.inject.Inject;

@Queue("forklift.replay.es")
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
        try {
            this.writer.poll(msg);
        } catch (Exception e) {
            if (writer != null && writer.getRecoveryCallback() != null) {
                writer.getRecoveryCallback().onError(e);
            }
            throw e;
        }
    }
}
