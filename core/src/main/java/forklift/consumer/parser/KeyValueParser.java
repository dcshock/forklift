package forklift.consumer.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KeyValueParser {
    private static Logger log = LoggerFactory.getLogger(KeyValueParser.class);

    public static Map<String, String> parse(String s) {
        final Map<String, String> result = new HashMap<>();

        if (s == null)
            return result;

        // Process each line separated by a newline.
        for (String line : s.split("\n")) {
            // Parse the line looking for the first equals to get the map key.
            final String[] parts = line.split("=");

            final String key = parts[0];
            if (key.trim().equals(""))
                continue;

            final StringBuilder builder = new StringBuilder();
            for (int i = 1; i < parts.length; i++)
                builder.append(parts[i]);

            final String value = builder.toString();

            if (result.containsKey(key))
                log.warn("key {} overwritten due to dupe value", key);

            log.info("'{}:{}'", key, value);
            result.put(key, value);
        }

        return result;
    }
}
