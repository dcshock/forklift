package forklift.consumer.lifecycle;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

public class TestAuditor2 {

    @LifeCycle(ProcessStep.Validating)
    public static void validate(MessageRunnable mr) throws InterruptedException {
        System.out.println("Validating: " + mr.getMsg().toString());
    }

    @LifeCycle(ProcessStep.Processing)
    @LifeCycle(ProcessStep.Retrying)
    public static void processing(MessageRunnable mr) {
        System.out.println("Processing: " + mr.getMsg().toString());
    }

    @LifeCycle(ProcessStep.Complete)
    public static void completed(MessageRunnable mr) {
        System.out.println("Completed: " + mr.getMsg().toString());
    }
}
