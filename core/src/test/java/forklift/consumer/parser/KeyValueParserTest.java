package forklift.consumer.parser;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

@RunWith(JUnit4.class)
public class KeyValueParserTest {
    @Test
    public void parseNull() {
        assertSame(0, KeyValueParser.parse(null).size());
        assertSame(0, KeyValueParser.parse("").size());
    }

    @Test
    public void parse() {
        String msg =
            "x=y\n" +
            "x=y\n" +
            "xy\n" +
            "y=hello this is a really cool message;";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue("y".equals(result.get("x")));
        assertTrue("hello this is a really cool message;".equals(result.get("y")));
        assertTrue("".equals(result.get("xy")));
        assertTrue(result.size() == 3);
    }

    @Test
    public void parse2() {
        String msg =
            "x=5\n" +
            "x=y\n" +
            "xy\n" +
            "&*F=\n" +
            "\n" +
            "\n" +
            "# y= test\tspace \n" +
            "y=hello this is a really cool message;\n" +
            "m=x=y+5\n" +
            "n=hello\\nworld\n" +
            " p       =!@#$%^&*()_+|}{?><,./-=`~'\"";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue("y".equals(result.get("x")));
        assertTrue("hello this is a really cool message;".equals(result.get("y")));
        assertTrue("".equals(result.get("xy")));
        assertTrue("".equals(result.get("&*F")));
        assertTrue("hello\\nworld".equals(result.get("n")));
        assertTrue("x=y+5".equals(result.get("m")));
        assertTrue(" test\tspace ".equals(result.get("# y")));
        assertTrue("!@#$%^&*()_+|}{?><,./-=`~'\"".equals(result.get("p")));

        assertTrue(result.size() == 8);
    }

    @Test
    public void parse3() {
        String msg =
            "=";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue(result.size() == 0);
    }

    @Test
    public void parse4() {
        String msg =
            "=========";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue(result.size() == 0);
    }

    @Test
    public void parse5() {
        String msg =
            "===*****------^^^^^";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue(result.size() == 0);
    }

    @Test
    public void parse6() {
        String msg =
            "     =     ==";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue(result.size() == 0);
    }

    @Test
    public void parseNLCR() {
        String msg =
            "x=y\n\r" +
            "a=123\n\r\n\r" +
            "name=Buzz Lightyear\n\r";

        final Map<String, String> result = KeyValueParser.parse(msg);
        assertTrue("y".equals(result.get("x")));
        assertTrue("123".equals(result.get("a")));
        assertTrue("Buzz Lightyear".equals(result.get("name")));
        assertTrue(result.size() == 3);
    }
}
