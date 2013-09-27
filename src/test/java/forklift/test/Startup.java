package forklift.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import forklift.ForkLift;
import forklift.exception.ForkLiftStartupException;

/**
 * Test the startup and shutdown of ForkLift to ensure that
 * spring contexts can be found, and that the project spins up
 * and down without error.
 *
 */
public class Startup {
    @Test
    public void classpathStart() 
      throws ForkLiftStartupException {
        ForkLift forklift = new ForkLift();
        forklift.start();
        forklift.shutdown();
    }
    
    @Test 
    public void classpathStartFail() {
        ForkLift forklift = new ForkLift();
        try {
            forklift.start("non-existent.xml");
            Assert.fail();
        } catch (ForkLiftStartupException e) {
        }
    }
    
    @Test
    public void fileStart() 
      throws IOException, ForkLiftStartupException {
        File f = File.createTempFile("forklift.", ".xml");
        f.deleteOnExit();

        BufferedInputStream is = 
            new BufferedInputStream(getClass().getResourceAsStream("/services.xml"));
        Assert.assertNotNull(is);
        
        FileWriter writer = new FileWriter(f);
        int b = -1;
        while ((b = is.read()) != -1) 
            writer.write(b);
        writer.close();
        
        ForkLift forklift = new ForkLift();
        forklift.start(f);
        forklift.shutdown();

        f.delete();
    }
}
