package forklift;

import java.io.File;

import org.junit.After;
import org.junit.Before;

import forklift.exception.StartupException;

public class ForkliftTest {
    protected Forklift forklift;
    
    @Before
    public void start() 
      throws StartupException {
        forklift = new Forklift();
        forklift.start();
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
