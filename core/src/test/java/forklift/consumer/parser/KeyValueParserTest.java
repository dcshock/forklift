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
}