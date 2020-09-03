package forklift;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * Properties can be added into forklift via this property file construct.
 *
 * @author David Thompson
 *
 */
public class ForkliftPropFile {
    File propertyFile;

    /**
     * Locate a properties file as a URL, file-system path, or resource
     *
     * @param classLoader - the classloader used to locate the properties file as a resource if not a URL
     * @param forkliftConfigFile - a string representing the properties file. Can be a URL, file-system location or name
     * @return a URL representing where the properties file was found.
     */
    public static URL findConfigFileURLFromSystemProperties(ClassLoader classLoader, String forkliftConfigFile) {
        if (forkliftConfigFile != null && forkliftConfigFile.length() > 0) {
            URL result = null;
            try {
                result = new URL(forkliftConfigFile);
                return result;
            } catch (MalformedURLException e) {
                // so, resource is not a URL:
                // attempt to get the resource from the class path
                result = classLoader.getResource(forkliftConfigFile);
                if (result != null) {
                    return result;
                }
                File f = new File(forkliftConfigFile);
                if (f.exists() && f.isFile()) {
                    try {
                        result = f.toURI().toURL();
                        return result;
                    } catch (MalformedURLException e1) {
                    }
                }
            }
        }
        return null;
    }

    /**
     * Load up a properties file and add all the properties to the system properties. Any
     * code in forklift can then access the properties via System.getProperty
     *
     * @param forkliftConfigFile - The filename of the properties file on the classpath or as a URL
     */
    public static void addPropertiesFromFile(String forkliftConfigFile) {
        Properties p = new Properties();
        URL url = findConfigFileURLFromSystemProperties(ForkliftServer.class.getClassLoader(), forkliftConfigFile);
        if (url != null) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
                p.load(in);
                p.stringPropertyNames().forEach((String name) -> {
                    String value = p.getProperty(name);
                    System.setProperty(name, value);
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
