package forklift.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

public class Listener {
    private String queue;
    private Class msgHandler;
    private AtomicBoolean running;
}
