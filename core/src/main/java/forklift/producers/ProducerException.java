package forklift.producers;

public class ProducerException extends Exception {
	public ProducerException(String s) {
        super(s);
    }
    public ProducerException(String s, Throwable e) {
        super(s, e);
    }
}
