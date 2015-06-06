package forklift.deployment;

import forklift.file.FileScanResult;
import forklift.file.FileScanner;
import forklift.file.FileStatus;
import forklift.properties.PropertiesManager;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * DeploymentWatch ties the file system to the deployment manager. A thread runner will
 * call this watch periodically to determine if any deployments have been created/removed
 * since the last scan.
 * @author mattconroy
 *
 */
public class DeploymentWatch implements Runnable {
    private FileScanner fileScan;
    private DeploymentEvents events;
    private DeploymentManager deploymentManager;
    private PropertiesManager properties;

    public DeploymentWatch(File dir, DeploymentEvents events) {
        this.events = events;

        if (!dir.isDirectory())
            throw new IllegalArgumentException("A directory must be specified to watch");

        fileScan = new FileScanner(dir);
        deploymentManager = new DeploymentManager();
        properties = new PropertiesManager();
    }

    @Override
    public synchronized void run() {
        final List<FileScanResult> results = fileScan.scan();
        for (FileScanResult result : results) {
            final File file = new File(fileScan.getDir(), result.getFilename());

            boolean jar = file.getName().endsWith(".jar") || file.getName().endsWith(".zip");
            boolean props = file.getName().endsWith(".properties") || file.getName().endsWith(".conf");

            if (result.getStatus() == FileStatus.Removed ||
                result.getStatus() == FileStatus.Modified) {

                if (jar)
                    events.onUndeploy(deploymentManager.unregisterDeployedFile(file));
                else if (props)
                    properties.deregister(file);
            }

            if (result.getStatus() == FileStatus.Added ||
                result.getStatus() == FileStatus.Modified) {
                try {
                    if (jar)
                        events.onDeploy(deploymentManager.registerDeployedFile(file));
                    else if (props)
                        properties.register(file);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Shutdown all running deployments.
     */
    public synchronized void shutdown() {
        deploymentManager.getAll().stream().forEach(deploy -> events.onUndeploy(deploy));
    }
}
