package forklift.consumer;

class FailedSystem extends System {
    FailedSystem(RequiredSystem system, long recoveryTime) {
        super(system, recoveryTime);
    }
}