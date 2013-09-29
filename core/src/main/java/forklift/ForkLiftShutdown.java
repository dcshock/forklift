package forklift;

/**
 * Helps ensure a complete shutdown of ForkLift when the JVM quits.
 */
public class ForkliftShutdown extends Thread {
    private Forklift forklift;
    
    public ForkliftShutdown(Forklift forklift) {
        this.forklift = forklift;
    }
    
    @Override
    public void run() {
        if (forklift.isRunning())
            forklift.shutdown();
    }
}
