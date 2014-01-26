package forklift;

import org.junit.After;
import org.junit.Before;

import forklift.Forklift;
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
}
