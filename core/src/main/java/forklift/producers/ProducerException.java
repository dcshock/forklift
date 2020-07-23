package forklift.producers;

public class ProducerException extends Exception {
    private static final long serialVersionUID = -7113872489074605962L;

    public ProducerException(String s) {
        super(s);
    }
    public ProducerException(String s, Throwable e) {
        super(s, e);
    }
}