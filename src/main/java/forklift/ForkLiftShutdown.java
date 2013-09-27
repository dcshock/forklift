package forklift;

public class ForkLiftShutdown extends Thread {
    private ForkLift forklift;
    
    public ForkLiftShutdown(ForkLift forklift) {
        this.forklift = forklift;
    }
    
    @Override
    public void run() {
        if (forklift.isRunning())
            forklift.shutdown();
    }
}
