package forklift;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import forklift.exception.StartupException;

/**
 * Test the startup and shutdown of ForkLift to ensure that
 * spring contexts can be found, and that the project spins up
 * and down without error.
 *
 */
public class Startup {
    @Test
    public void start()
      throws IOException, StartupException, InterruptedException {
        final Forklift forklift = new Forklift();

        forklift.start();
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

    @Test
    public void mainStart()
      throws IOException, InterruptedException, StartupException {
        final Forklift forklift = Forklift.mainWithTestHook(null);

        // Check startup
        int count = 20;
        while (!forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        Assert.assertTrue(forklift.isRunning());

        // Check shutdown
        forklift.shutdown();
        count = 20;
        while (forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        Assert.assertFalse(forklift.isRunning());
    }
}
