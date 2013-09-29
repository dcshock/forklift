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
        
        Forklift forklift = new Forklift();
        forklift.start(f);
        forklift.shutdown();

        f.delete();
    }
}
