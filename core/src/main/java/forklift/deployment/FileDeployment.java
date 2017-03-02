package forklift.deployment;

import forklift.classloader.ChildFirstClassLoader;
import forklift.classloader.RunAsClassLoader;
import forklift.decorators.CoreService;
import forklift.decorators.Queue;
import forklift.decorators.Queues;
import forklift.decorators.Service;
import forklift.decorators.Topic;
import forklift.decorators.Topics;
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

public class FileDeployment implements Deployment{
    private Set<Class<?>> queues = new HashSet<>();
    private Set<Class<?>> topics = new HashSet<>();
    private Set<Class<?>> services = new HashSet<>();
    private Set<Class<?>> coreServices = new HashSet<>();
    private ClassLoader cl;

    private File deployedFile;
    private Reflections reflections;

    public FileDeployment() {

    }


    public FileDeployment(File deployedFile)
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

        // TODO - we should cleanup temp jars when the deploy is thrown away.

        final URL[] urls = jarUrls.toArray(new URL[0]);

        // Assign a new classloader to this deployment.
        cl = new ChildFirstClassLoader(urls, getClass().getClassLoader());

        // Reflect the deployment to determine if there are any consumers
        // annotated.
        reflections = new Reflections(new ConfigurationBuilder()
            .addClassLoader(cl)
            .setUrls(urls));

        RunAsClassLoader.run(cl, () -> {
            coreServices.addAll(reflections.getTypesAnnotatedWith(CoreService.class));
            queues.addAll(reflections.getTypesAnnotatedWith(Queue.class));
            queues.addAll(reflections.getTypesAnnotatedWith(Queues.class));
            services.addAll(reflections.getTypesAnnotatedWith(Service.class));
            topics.addAll(reflections.getTypesAnnotatedWith(Topic.class));
            topics.addAll(reflections.getTypesAnnotatedWith(Topics.class));
        });

        if (coreServices.size() > 0 && (queues.size() > 0 || topics.size() > 0 || services.size() > 0))
            throw new IOException("Invalid core service due to queues/topics/services being deployed along side.");
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

    @Override
    public Set<Class<?>> getCoreServices() {
        return coreServices;
    }

    @Override
    public Set<Class<?>> getServices() {
        return services;
    }

    @Override
    public Set<Class<?>> getQueues() {
        return queues;
    }

    @Override
    public Set<Class<?>> getTopics() {
        return topics;
    }

    public Reflections getReflections() {
        return reflections;
    }

    @Override
    public boolean equals(Object o) {
        if (((FileDeployment)o).getDeployedFile().equals(deployedFile))
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "FileDeployment [queues=" + queues + ", topics=" + topics + ", cl="
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
