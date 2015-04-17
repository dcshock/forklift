package forklift.deployment;

import forklift.file.FileScanResult;
import forklift.file.FileScanner;
import forklift.file.FileStatus;

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

    public DeploymentWatch(File dir, DeploymentEvents events) {
        this.events = events;

        if (!dir.isDirectory())
            throw new IllegalArgumentException("A directory must be specified to watch");

        fileScan = new FileScanner(dir);
        deploymentManager = new DeploymentManager();
    }

    @Override
    public void run() {
        final List<FileScanResult> results = fileScan.scan();
        for (FileScanResult result : results) {
            final File file = new File(fileScan.getDir(), result.getFilename());

            if (result.getStatus() == FileStatus.Removed ||
                result.getStatus() == FileStatus.Modified)
                events.onUndeploy(deploymentManager.unregisterDeployedFile(file));

            if (result.getStatus() == FileStatus.Added ||
                result.getStatus() == FileStatus.Modified) {
                try {
                    events.onDeploy(deploymentManager.registerDeployedFile(file));
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
