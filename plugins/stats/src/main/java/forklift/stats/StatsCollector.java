package forklift.stats;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

public class StatsCollector {
    @LifeCycle(value=ProcessStep.Pending)
    public void pending(MessageRunnable mr) {
        setProp(mr, "pending");
    }

    @LifeCycle(value=ProcessStep.Validating)
    public void validating(MessageRunnable mr) {
        setProp(mr, "validating");
    }

    @LifeCycle(value=ProcessStep.Invalid)
    public void invalid(MessageRunnable mr) {
        setProp(mr, "invalid");
        mr.getMsg().getProperties().put("forklift-stats-validating-total",
            (System.currentTimeMillis() - (long)mr.getMsg().getProperties().get("forklift-stats-validating")));
    }

    @LifeCycle(value=ProcessStep.Processing)
    public void processing(MessageRunnable mr) {
        setProp(mr, "processing");
    }

    @LifeCycle(value=ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        setProp(mr, "complete");
        mr.getMsg().getProperties().put("forklift-stats-processing-total",
            (System.currentTimeMillis() - (long)mr.getMsg().getProperties().get("forklift-stats-processing")));
    }

    @LifeCycle(value=ProcessStep.Error)
    public void error(MessageRunnable mr) {
        setProp(mr, "error");
        mr.getMsg().getProperties().put("forklift-stats-processing-total",
            (System.currentTimeMillis() - (long)mr.getMsg().getProperties().get("forklift-stats-processing")));
    }

    @LifeCycle(value=ProcessStep.Retrying)
    public void retry(MessageRunnable mr) {
        setProp(mr, "retrying");
    }

    private void setProp(MessageRunnable mr, String prop) {
        mr.getMsg().getProperties().put("forklift-stats-" + prop, System.currentTimeMillis());
    }
}
