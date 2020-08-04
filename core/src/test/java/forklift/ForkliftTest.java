package forklift;

import forklift.connectors.ForkliftConnectorI;
import forklift.exception.StartupException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

import java.io.File;
import java.net.URL;

public class ForkliftTest {
    protected Forklift forklift;

    @BeforeAll
    public void start()
      throws StartupException {
        forklift = new Forklift();
        forklift.start(Mockito.mock(ForkliftConnectorI.class));
    }

    @AfterAll
    public void stop() {
      forklift.shutdown();
    }

    public static File testJar() {
        return findResourceFile("forklift-test-consumer-0.1.jar");
    }

    public static File testJarJar() {
        return findResourceFile("forklift-jarjar-consumer-0.1-binks.jar");
    }

    public static File testMultiTQJar() {
        return findResourceFile("forklift-multitq-consumer-0.1.jar");
    }

    private static File findResourceFile(String fileName){
        File a = new File(fileName);
        if (a.exists())
            return a;
        File b = new File(fileName);
        if(b.exists())
            return b;
        URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);
        if(url != null) {
            File c = new File(url.getPath());
            if(c.exists()){
                return c;
            }
        }
        return null;
    }

}
