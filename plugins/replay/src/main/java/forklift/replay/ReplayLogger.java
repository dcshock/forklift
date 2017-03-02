package forklift.replay;

import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.jms.JMSException;

public class ReplayLogger {
    private final ReplayWriter writer;

    public ReplayLogger(File dir) throws FileNotFoundException {
        this.writer = new ReplayWriter(new File(dir, "replay." + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) + ".log"));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        this.writer.start();
    }

    @LifeCycle(value=ProcessStep.Pending, annotation=Replay.class)
    public void pending(MessageRunnable mr) {
        theRest(mr, ProcessStep.Pending);
    }

    @LifeCycle(value=ProcessStep.Validating, annotation=Replay.class)
    public void validating(MessageRunnable mr) {
        theRest(mr, ProcessStep.Validating);
    }

    @LifeCycle(value=ProcessStep.Invalid, annotation=Replay.class)
    public void invalid(MessageRunnable mr) {
        theRest(mr, ProcessStep.Invalid);
    }

    @LifeCycle(value=ProcessStep.Processing, annotation=Replay.class)
    public void processing(MessageRunnable mr) {
        theRest(mr, ProcessStep.Processing);
    }

    @LifeCycle(value=ProcessStep.Complete, annotation=Replay.class)
    public void complete(MessageRunnable mr) {
        theRest(mr, ProcessStep.Complete);
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Replay.class)
    public void error(MessageRunnable mr) {
        theRest(mr, ProcessStep.Error);
    }

    @LifeCycle(value=ProcessStep.Retrying, annotation=Replay.class)
    public void retrying(MessageRunnable mr) {
        theRest(mr, ProcessStep.Retrying);
    }

    @LifeCycle(value=ProcessStep.MaxRetriesExceeded, annotation=Replay.class)
    public void maxRetries(MessageRunnable mr) {
        theRest(mr, ProcessStep.MaxRetriesExceeded);
    }

    public void theRest(MessageRunnable mr, ProcessStep step) {
        final ForkliftMessage msg = mr.getMsg();
        final Consumer consumer = mr.getConsumer();
        final ReplayMsg replayMsg = new ReplayMsg();
        try {
            replayMsg.messageId = msg.getJmsMsg().getJMSMessageID();
        } catch (JMSException ignored) {
        }
        replayMsg.text = msg.getMsg();
        replayMsg.headers = msg.getHeaders();
        replayMsg.step = step;
        replayMsg.properties = msg.getProperties();
        replayMsg.errors = mr.getErrors();

        if (consumer.getQueue() != null)
            replayMsg.queue = consumer.getQueue().value();

        if (consumer.getTopic() != null)
            replayMsg.topic = consumer.getTopic().value();

        this.writer.put(replayMsg);
    }
}