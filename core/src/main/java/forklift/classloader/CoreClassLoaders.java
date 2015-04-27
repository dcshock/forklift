package forklift.classloader;

import forklift.Registrar;

public class CoreClassLoaders extends Registrar<ClassLoader> {
    private static CoreClassLoaders c;

    public static synchronized CoreClassLoaders getInstance() {
        if (c == null)
            c = new CoreClassLoaders();
        return c;
    }
}
