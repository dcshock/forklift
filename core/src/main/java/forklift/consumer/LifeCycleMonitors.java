package forklift.consumer;

import forklift.decorators.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LifeCycleMonitors {
    private static final Logger log = LoggerFactory.getLogger(LifeCycleMonitors.class);

    private static class Monitor {
        Class<?> clazz;
        Method method;
        Object instance;
        Class<? extends Annotation> annotation;
    }

    private static AtomicInteger calls;
    private static final Map<ProcessStep, List<Monitor>> monitors;
    static {
        calls = new AtomicInteger(0);

        // Let's make sure we don't have to initialize anything at the root level later.
        monitors = new HashMap<>();
        monitors.put(ProcessStep.Pending, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Validating, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Invalid, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Processing, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Retrying, new ArrayList<Monitor>());
        monitors.put(ProcessStep.MaxRetriesExceeded, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Error, new ArrayList<Monitor>());
        monitors.put(ProcessStep.Complete, new ArrayList<Monitor>());
    }

    public static void register(Class<?> clazz) {
        register(clazz, null);
    }

    public static void register(Object existingInstance) {
        register(existingInstance.getClass(), existingInstance);
    }

    private static void register(Class<?> clazz, Object existingInstance) {
        synchronized (monitors) {
            while (calls.get() > 0) {
                try {
                    monitors.wait(10);
                } catch (InterruptedException ignored) {}
            }
            if (calls.get() != 0)
                throw new RuntimeException("Registering LifeCycleMonitor during active call.");

            // For methods that aren't static we'll need to have an instance around.
            Object instance = existingInstance;

            // Get all of the methods that may have lifecycle annotations.
            for (Method m : clazz.getDeclaredMethods()) {
                // Check to see if we need an instance around.
                final boolean staticMethod = Modifier.isStatic(m.getModifiers());
                if (!staticMethod && instance == null) {
                    try {
                        if (existingInstance == null)
                            instance = clazz.newInstance();
                    } catch (Exception ignored) {
                        log.error("", ignored);
                        return;
                    }
                }

                for (Annotation a : m.getAnnotationsByType(LifeCycle.class)) {
                    log.info("Adding Monitor.... {}-{}", clazz, a);
                    m.setAccessible(true);

                    final Monitor monitor = new Monitor();
                    monitor.clazz = clazz;
                    monitor.method = m;

                    // Only store the instance if this isn't static so that the method can be
                    // called later on.
                    if (!staticMethod)
                        monitor.instance = instance;

                    final LifeCycle lifeCycle = (LifeCycle)a;
                    monitor.annotation = lifeCycle.annotation();
                    monitors.get(lifeCycle.value()).add(monitor);
                }
            }
        }
    }

    public static void deregister(final Class<?> clazz) {
        synchronized (monitors) {
            while (calls.get() > 0) {
                try {
                    monitors.wait(10);
                } catch (InterruptedException ignored) {}
            }
            if (calls.get() != 0)
                throw new RuntimeException("Registering LifeCycleMonitor during active call.");

            // Stream through the monitors hash, and remove any underlying list elements
            // that are represented by the deregistering class.
            monitors.values().stream().forEach(list -> {
                // Use an iterator to avoid concurrent mod exceptions
                final Iterator<Monitor> it = list.iterator();
                while (it.hasNext()) {
                    final Monitor monitor = it.next();
                    if (monitor.clazz == clazz) {
                        log.info("Removing Monitor.... {}-{}", monitor.clazz, monitor.method);
                        it.remove();
                    }
                }
            });
        }
    }

    public static void call(ProcessStep step, MessageRunnable mr) {
        if (calls.get() == 0) {
            synchronized (monitors) {
                calls.incrementAndGet();
            }
        } else {
            calls.incrementAndGet();
        }

        try {
            // Find calls that match the step we're currently processing.
            monitors.keySet().stream().filter(key -> key == step)
                .map(key -> monitors.get(key))
                .forEach(list -> {
                    list.forEach(monitor -> {
                        try {
                            // In the case of static methods the registration() method leaves the
                            // instance as null.
                            if (monitor.annotation == Annotation.class ||
                                mr.getConsumer().getMsgHandler().isAnnotationPresent(monitor.annotation)) {
                                if (monitor.method.getParameterCount() == 1)
                                    monitor.method.invoke(monitor.instance, mr);
                                else
                                    monitor.method.invoke(monitor.instance, mr, mr.getConsumer().getMsgHandler().getAnnotation(monitor.annotation));
                            }
                        } catch (Throwable e) {
                            log.error("Error invoking LifeCycle Monitor", e);
                        }
                    });
                });
        } finally {
            calls.decrementAndGet();
        }
    }

    public static Integer getCalls() {
        return calls.get();
    }
}
