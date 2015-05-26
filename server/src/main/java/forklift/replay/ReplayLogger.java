package forklift.replay;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ReplayLogger {
    private final ReplayWriter writer;

    public ReplayLogger() throws FileNotFoundException {
        this.writer = new ReplayWriter(new File("replay.log"));

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

    @LifeCycle(ProcessStep.Pending)
    public void pending(MessageRunnable mr) {
        theRest(mr, ProcessStep.Pending);
    }

    @LifeCycle(ProcessStep.Validating)
    public void validating(MessageRunnable mr) {
        theRest(mr, ProcessStep.Validating);
    }

    @LifeCycle(ProcessStep.Invalid)
    public void invalid(MessageRunnable mr) {
        theRest(mr, ProcessStep.Invalid);
    }

    @LifeCycle(ProcessStep.Processing)
    public void processing(MessageRunnable mr) {
        theRest(mr, ProcessStep.Processing);
    }

    @LifeCycle(ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        theRest(mr, ProcessStep.Complete);
    }

    @LifeCycle(ProcessStep.Error)
    public void error(MessageRunnable mr) {
        theRest(mr, ProcessStep.Error);
    }

    @LifeCycle(ProcessStep.Retrying)
    public void retrying(MessageRunnable mr) {
        theRest(mr, ProcessStep.Retrying);
    }

    @LifeCycle(ProcessStep.MaxRetriesExceeded)
    public void maxRetries(MessageRunnable mr) {
        theRest(mr, ProcessStep.MaxRetriesExceeded);
    }

    public void theRest(MessageRunnable mr, ProcessStep step) {
        this.writer.write(mr.getMsg(), step);
    }
}