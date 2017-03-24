package forklift.integration;

/**
 * Created by afrieze on 3/9/17.
 */
public class TestMessage {
    private String text;

    private int someNumber;

    public TestMessage(){}

    public TestMessage(String text, int someNumber) {
        this.text = text;
        this.someNumber = someNumber;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getSomeNumber() {
        return someNumber;
    }

    public void setSomeNumber(int someNumber) {
        this.someNumber = someNumber;
    }
}
