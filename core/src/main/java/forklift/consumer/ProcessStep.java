package forklift.consumer;

public enum ProcessStep {
    Pending,
    Validating,
    Processing,
    Invalid,

    // TODO Not Yet Implemented
    Retrying,
    MaxRetriesExceeded,

    Error,
    Complete
}