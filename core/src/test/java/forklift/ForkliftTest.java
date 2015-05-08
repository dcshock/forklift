package forklift;

import forklift.connectors.ForkliftConnectorI;
import forklift.exception.StartupException;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.File;

public class ForkliftTest {
    protected Forklift forklift;

    @Before
    public void start()
      throws StartupException {
        forklift = new Forklift();
        forklift.start(Mockito.mock(ForkliftConnectorI.class));
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

    public static File testJarJar() {
        File a = new File("src/test/resources/forklift-jarjar-consumer-0.1-binks.jar");
        if (a.exists())
            return a;
        File b = new File("core/src/test/resources/forklift-jarjar-consumer-0.1-binks.jar");
        if(b.exists())
            return b;
        return null;
    }

    public static File testMultiTQJar() {
        File a = new File("src/test/resources/forklift-multitq-consumer-0.1.jar");
        if (a.exists())
            return a;
        File b = new File("core/src/test/resources/forklift-multitq-consumer-0.1.jar");
        if(b.exists())
            return b;
        return null;
    }
}
