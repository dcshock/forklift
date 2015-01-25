package forklift.consumer;

public enum ProcessStep {
	Pending,
	Validating,
	Invalid, 
	Processing,
	Error,
	Complete
}
