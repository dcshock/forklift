package forklift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.MalformedURLException;

import org.junit.jupiter.api.Test;

public class ForkliftOptsTest {

    @Test
    public void testFindPropFile() throws MalformedURLException {
       var url = ForkliftPropFile.findConfigFileURLFromSystemProperties(this.getClass().getClassLoader(), "file:./test.properties");
       assertEquals("./test.properties", url.getFile());

       url = ForkliftPropFile.findConfigFileURLFromSystemProperties(this.getClass().getClassLoader(), "test.properties");
       assertTrue(url.getFile().contains("test.properties"));

       url = ForkliftPropFile.findConfigFileURLFromSystemProperties(this.getClass().getClassLoader(), "shouldnt-find-this-filename");
       assertNull(url);

       url = ForkliftPropFile.findConfigFileURLFromSystemProperties(this.getClass().getClassLoader(), null);
       assertNull(url);

       url = ForkliftPropFile.findConfigFileURLFromSystemProperties(this.getClass().getClassLoader(), "");
       assertNull(url);
    }

    @Test
    public void testLoadProps() {
        int sizeBefore, sizeAfter;

        // Make sure no props are added and that we don't bomb.
        sizeBefore = System.getProperties().size();
        ForkliftPropFile.addPropertiesFromFile("");
        sizeAfter = System.getProperties().size();
        assertEquals(sizeBefore, sizeAfter);

        sizeBefore = System.getProperties().size();
        ForkliftPropFile.addPropertiesFromFile(null);
        sizeAfter = System.getProperties().size();
        assertEquals(sizeBefore, sizeAfter);

        sizeBefore = System.getProperties().size();
        ForkliftPropFile.addPropertiesFromFile("test.properties");
        sizeAfter = System.getProperties().size();
        assertEquals(sizeBefore + 1, sizeAfter);

        assertThrows(IllegalArgumentException.class, () -> {
            ForkliftPropFile.addPropertiesFromFile("bad-props-file.properties");
        });

        sizeBefore = System.getProperties().size();
        ForkliftPropFile.addPropertiesFromFile("example.properties");
        sizeAfter = System.getProperties().size();
        assertEquals(sizeBefore + 9, sizeAfter);
        assertEquals("prod", System.getProperty("app.environment"));
        assertEquals("true", System.getProperty("datadog.lifecycle.counter.pending.testQueue"));
    }
}
