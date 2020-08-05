package forklift;

import forklift.connectors.ForkliftConnectorI;
import forklift.exception.StartupException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

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

        forklift.start(Mockito.mock(ForkliftConnectorI.class));
        int count = 20;
        while (!forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        assertTrue(forklift.isRunning());

        forklift.shutdown();
        count = 20;
        while (forklift.isRunning() && count-- > 0)
            Thread.sleep(250);
        assertFalse(forklift.isRunning());
    }
}
