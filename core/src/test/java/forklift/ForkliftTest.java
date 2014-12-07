package forklift;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.springframework.util.Assert;

import forklift.exception.StartupException;
import forklift.spring.ContextManager;

public class ForkliftTest {
    protected Forklift forklift;

    @Before
    public void start()
      throws StartupException {
        forklift = new Forklift();
        forklift.start();

        Assert.notNull(ContextManager.getContext(), "ContextManager.getContext() was null");
    }

    @After
    public void stop() {
      forklift.shutdown();
    }

    public static File testJar() {
        File a = new File("src/test/resources/forklift-test-consumer-0.1.jar");
        if (a.exists())
            return a;
        File b = new File("core/src/test/resources/forklift-test-consumer-0.1.jar");
        if(b.exists())
            return b;
        return null;
    }
}
