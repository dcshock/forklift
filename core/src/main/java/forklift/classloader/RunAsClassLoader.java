package forklift.classloader;

/**
 * Swaps the current thread's classloader to the specified class loader, and
 * then ensures it is returned to the previous state once the runnable is completed.
 * @author mconroy
 *
 */
public class RunAsClassLoader {
    public static void run(ClassLoader cl, Runnable r) {
        final ClassLoader before = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(cl);
            r.run();
        } finally {
            Thread.currentThread().setContextClassLoader(before);
        }
    }
}
