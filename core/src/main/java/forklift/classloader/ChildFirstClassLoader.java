package forklift.classloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class ChildFirstClassLoader extends URLClassLoader {
    private ClassLoader system;

    public ChildFirstClassLoader(URL[] classpath, ClassLoader parent) {
        super(classpath, parent);
        system = getSystemClassLoader();
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
        // First, check if the class has already been loaded
        Class<?> c = findLoadedClass(name);

        // There are some classes that we can't go to the child first to resolve. In these instances we'll intercept the call
        // and delegate it to the system classloader. All of these relate to base jre libraries that are bundled in java.
        List<String> libraries = Arrays.asList("java", "com.sun", "sun", "org.xml", "org.w3c");
        if (libraries.stream().anyMatch(s -> name.startsWith(s))) {
            try {
                c = system.loadClass(name);
            } catch (ClassNotFoundException e3) {
            }
        }

        if (c == null) {
            try {
                // checking local
                c = findClass(name);
            } catch (ClassNotFoundException e) {
                // checking parent
                // This call to loadClass may eventually call findClass
                // again, in case the parent doesn't find anything.
                try {
                    c = super.loadClass(name, resolve);
                } catch (ClassNotFoundException e2) {
                    try {
                        // Try core class loaders
                        for (ClassLoader cl : CoreClassLoaders.getInstance().getAll()) {
                            c = cl.loadClass(name);
                            if (c != null)
                                break;
                        }
                    } catch (ClassNotFoundException e3) {
                        if (system != null) {
                            // checking system: jvm classes, endorsed, cmd classpath,
                            // etc.
                            c = system.loadClass(name);
                        }
                    }
                }
            }
        }
        if (resolve)
            resolveClass(c);

        if (c == null)
            throw new ClassNotFoundException(name);

        return c;
    }

    @Override
    public URL getResource(String name) {
        URL url = findResource(name);
        if (url == null)
            url = super.getResource(name);

        if (url == null && system != null)
            url = system.getResource(name);

        return url;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        /**
         * Similar to super, but local resources are enumerated before parent
         * resources
         */
        Enumeration<URL> systemUrls = null;
        if (system != null) {
            systemUrls = system.getResources(name);
        }
        Enumeration<URL> localUrls = findResources(name);
        Enumeration<URL> parentUrls = null;
        if (getParent() != null) {
            parentUrls = getParent().getResources(name);
        }
        final List<URL> urls = new ArrayList<URL>();
        if (localUrls != null) {
            while (localUrls.hasMoreElements()) {
                URL local = localUrls.nextElement();
                urls.add(local);
            }
        }
        if (systemUrls != null) {
            while (systemUrls.hasMoreElements()) {
                urls.add(systemUrls.nextElement());
            }
        }
        if (parentUrls != null) {
            while (parentUrls.hasMoreElements()) {
                urls.add(parentUrls.nextElement());
            }
        }
        return new Enumeration<URL>() {
            Iterator<URL> iter = urls.iterator();

            public boolean hasMoreElements() {
                return iter.hasNext();
            }

            public URL nextElement() {
                return iter.next();
            }
        };
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        URL url = getResource(name);
        try {
            return url != null ? url.openStream() : null;
        } catch (IOException e) {
        }
        return null;
    }
}
