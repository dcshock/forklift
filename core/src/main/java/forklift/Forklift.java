package forklift;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.exception.StartupException;
import forklift.spring.ContextManager;

/**
 * Main ForkLift application instance. ForkLift is started here
 * and stopped here.
 */
public class Forklift {
    private static Logger log = LoggerFactory.getLogger("ForkLift");

    private AtomicBoolean running = new AtomicBoolean(false);

    public Forklift() {
        log.debug("Creating ForkLift");
    }

    public synchronized void start()
      throws StartupException {
        start(ForkliftConfig.class);
    }

    public synchronized void start(Class<?> config)
      throws StartupException {
        log.debug("Initializing Spring Context");
        ContextManager.start(config);
        running.set(true);
    }

    public void shutdown() {
        if (!running.getAndSet(false))
            return;

        ContextManager.stop();
    }

    public boolean isRunning() {
        return running.get();
    }

    public static void main(String args[])
      throws StartupException {
        final Forklift forklift = mainWithTestHook(args);
        if (!forklift.isRunning())
            throw new RuntimeException("Unable to start Forklift.");
    }

    public static Forklift mainWithTestHook(String args[])
      throws StartupException {
        final Forklift forklift = new Forklift();
        forklift.start();
        return forklift;
    }
}
