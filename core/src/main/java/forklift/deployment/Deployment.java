package forklift.deployment;

import forklift.classloader.ChildFirstClassLoader;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class Deployment {
    private Set<Class<?>> queues = new HashSet<Class<?>>();
    private Set<Class<?>> topics = new HashSet<Class<?>>();
    private ClassLoader cl;

    private File deployedFile;
    private Reflections reflections;

    public Deployment() {

    }

    public Deployment(File deployedFile)
      throws IOException {
        if (deployedFile == null)
            throw new IOException("Missing file");

        this.deployedFile = deployedFile;

        if (!deployedFile.getName().endsWith(".jar") && !deployedFile.getName().endsWith(".zip")) {
            throw new IOException("Unhandled file type");
        }

        // Read jars out of the deployed file.
        final JarFile jar = new JarFile(deployedFile);
        final List<URL> jarUrls = jar.stream().filter(entry -> {
           return entry.getName().endsWith(".jar") || entry.getName().endsWith(".zip");
        }).map(entry -> {
            try {
                return jarEntryAsUri(jar, entry).toURL();
            } catch (Exception e) {
                return null;
            }
        }).collect(Collectors.toList());
        jarUrls.add(deployedFile.toURI().toURL());

        final URL[] urls = jarUrls.toArray(new URL[0]);

        // Assign a new classloader to this deployment.
        cl = new ChildFirstClassLoader(urls, getClass().getClassLoader());

        // Reflect the deployment to determine if there are any consumers
        // annotated.
        reflections = new Reflections(new ConfigurationBuilder()
            .addClassLoader(cl)
            .setUrls(urls));

        queues.addAll(reflections.getTypesAnnotatedWith(Queue.class));
        topics.addAll(reflections.getTypesAnnotatedWith(Topic.class));
    }

    public boolean isJar() {
        return deployedFile.getPath().endsWith(".jar");
    }

    public boolean isClass() {
        return deployedFile.getPath().endsWith(".class");
    }

    public File getDeployedFile() {
        return deployedFile;
    }

    public void setDeployedFile(File deployedFile) {
        this.deployedFile = deployedFile;
    }

    public ClassLoader getClassLoader() {
        return cl;
    }

    public Set<Class<?>> getQueues() {
        return queues;
    }

    public Set<Class<?>> getTopics() {
        return topics;
    }

    @Override
    public boolean equals(Object o) {
        if (((Deployment)o).getDeployedFile().equals(deployedFile))
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "Deployment [queues=" + queues + ", topics=" + topics + ", cl="
                + cl + ", deployedFile=" + deployedFile + ", reflections="
                + reflections + "]";
    }

    private static URI jarEntryAsUri(JarFile jarFile, JarEntry jarEntry)
      throws IOException {
        if (jarFile == null || jarEntry == null)
            throw new IOException("Invalid jar file or entry");

        InputStream input = null;
        OutputStream output = null;
        try {
            String name = jarEntry.getName().replace('/', '_');
            int i = name.lastIndexOf(".");
            String extension = i > -1 ? name.substring(i) : "";
            File file = File.createTempFile(
                name.substring(0, name.length() - extension.length()) +
                ".", extension);
            file.deleteOnExit();
            input = jarFile.getInputStream(jarEntry);
            output = new FileOutputStream(file);
            int readCount;
            byte[] buffer = new byte[4096];
            while ((readCount = input.read(buffer)) != -1) {
                output.write(buffer, 0, readCount);
            }
            return file.toURI();
        } finally {
            if (input != null)
                input.close();
            if (output != null)
                output.close();
        }
    }
}
