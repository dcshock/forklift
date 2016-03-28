package forklift.consumer;

class System {
    private RequiredSystem system;
    private long recoveryTime;

    System(RequiredSystem system, long recoveryTime) {
        this.system = system;
        this.recoveryTime = recoveryTime;
    }

    RequiredSystem system() {
        return this.system;
    }

    long getRecoveryTime() {
        return this.recoveryTime;
    }
}
