package forklift.consumer.lifecycle;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

public class BadAuditor {

    public BadAuditor() {
        throw new RuntimeException("purposely blowing up");
    }

    @LifeCycle(ProcessStep.Validating)
    public static void validate(MessageRunnable mr) throws InterruptedException {
        throw new RuntimeException("purposely blow up on validate");
    }
}
