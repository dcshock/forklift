package forklift.consumer;

public enum ProcessStep {
	Pending,
	Validating,
	Invalid, 
	Processing,
	Retrying,
	Error,
	Complete
}
