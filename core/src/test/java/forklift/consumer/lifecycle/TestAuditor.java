package forklift.consumer.lifecycle;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

public class TestAuditor {

    @LifeCycle(ProcessStep.Validating)
    public static void validate(MessageRunnable mr) throws InterruptedException {
        System.out.println("Validating: " + mr.getMsg().toString());
        // Want to pause during validation to slow system down for tests.
        Thread.sleep(500);
    }

    @LifeCycle(ProcessStep.Processing)
    @LifeCycle(ProcessStep.Retrying)
    public static void processing(MessageRunnable mr) throws InterruptedException {
        System.out.println("Processing: " + mr.getMsg().toString());
        // Want to pause during validation to slow system down for tests.
        Thread.sleep(500);
    }

    @LifeCycle(ProcessStep.Complete)
    public static void completed(MessageRunnable mr) {
        System.out.println("Completed: " + mr.getMsg().toString());
    }
}
