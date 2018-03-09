package forklift;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import forklift.connectors.ForkliftConnectorI;
import forklift.exception.StartupException;

/**
 * Test the startup and shutdown of ForkLift to ensure that
 * spring contexts can be found, and that the project spins up
 * and down without error.
 *
 */
public class Startup {
    
    public void start()
      throws IOException, StartupException, InterruptedException {
        final Forklift forklift = new Forklift();

        forklift.start(Mockito.mock(ForkliftConnectorI.class));
        int count = 20;
        while (!forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        Assert.assertTrue(forklift.isRunning());

        forklift.shutdown();
        count = 20;
        while (forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        Assert.assertFalse(forklift.isRunning());
    }
}
