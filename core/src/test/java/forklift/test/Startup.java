package forklift.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import forklift.Forklift;
import forklift.exception.ForkliftStartupException;

/**
 * Test the startup and shutdown of ForkLift to ensure that
 * spring contexts can be found, and that the project spins up
 * and down without error.
 *
 */
public class Startup {
    @Test
    public void classpathStart() 
      throws ForkliftStartupException {
        Forklift forklift = new Forklift();
        forklift.start();
        forklift.shutdown();
    }
    
    @Test 
    public void classpathStartFail() {
        Forklift forklift = new Forklift();
        try {
            forklift.start("non-existent.xml");
            Assert.fail();
        } catch (ForkliftStartupException e) {
        }
    }
    
    @Test
    public void fileStart() 
      throws IOException, ForkliftStartupException {
        Forklift forklift = new Forklift();
        forklift.start(createTempConfig());
        forklift.shutdown();
    }
    
    @Test
    public void mainStart() 
      throws IOException, InterruptedException, ForkliftStartupException {
        final Forklift forklift = Forklift.mainWithTestHook(new String[] {
                createTempConfig().getPath()
        });
        
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
    
    @Test(expected = ForkliftStartupException.class) 
    public void mainStartFail() 
      throws ForkliftStartupException {
        Forklift.mainWithTestHook(new String[] {
                "/tmp/test.does.not.exist"
        });
    }
    
    private File createTempConfig() 
      throws IOException {
        final File f = File.createTempFile("forklift.", ".xml");
        f.deleteOnExit();

        final BufferedInputStream is = 
            new BufferedInputStream(getClass().getResourceAsStream("/services.xml"));
        Assert.assertNotNull(is);
        
        final FileWriter writer = new FileWriter(f);
        int b = -1;
        while ((b = is.read()) != -1) 
            writer.write(b);
        writer.close();
        
        return f;
    }
}
