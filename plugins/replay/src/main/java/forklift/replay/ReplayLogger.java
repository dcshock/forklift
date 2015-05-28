package forklift.replay;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ReplayLogger {
    private final ReplayWriter writer;

    public ReplayLogger() throws FileNotFoundException {
        this.writer = new ReplayWriter(new File("replay." + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) + ".log"));

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
        this.writer.write(mr.getConsumer(), mr.getMsg(), step, mr.getErrors());
    }
}