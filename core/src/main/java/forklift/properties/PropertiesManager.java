package forklift.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesManager {
    private static final Logger log = LoggerFactory.getLogger(PropertiesManager.class);

    private static final Map<String, Properties> deployments = new HashMap<>();

    public void register(File deployment) {
        log.info("Deploying: " + deployment);

        if (!deployment.getName().endsWith(".properties") &&
            !deployment.getName().endsWith(".conf")) {
            log.warn("Invalid properties file.");
            return;
        }

        // Load and store properties.
        FileReader fr = null;
        try {
            fr = new FileReader(deployment);

            final Properties props = new Properties();
            props.load(fr);

            synchronized (deployments) {
               deployments.put(getName(deployment), props);
            }
        } catch (IOException e) {
            log.warn("File didn't exist while attempting to read.");
            return;
        } catch (IllegalArgumentException e) {
            log.warn("Invalid properties file, please check syntax.");
            return;
        } finally {
            try {
                if (fr != null)
                    fr.close();
            } catch (IOException ignored) {}
        }
    }

    public void deregister(File deployment) {
        log.info("Undeploying: " + deployment);
        synchronized (deployments) {
            deployments.remove(getName(deployment));
        }
    }

    public static Properties get(String name) {
        synchronized (deployments) {
            return deployments.get(name);
        }
    }

    private String getName(File f) {
        return f.getName().split("\\.")[0];
    }
}
