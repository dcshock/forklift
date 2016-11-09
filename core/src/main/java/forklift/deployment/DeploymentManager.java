package forklift.deployment;

import forklift.Registrar;

import java.io.File;
import java.io.IOException;

public class DeploymentManager extends Registrar<Deployment> {
    public synchronized FileDeployment registerDeployedFile(File f)
      throws IOException {
        final FileDeployment d = new FileDeployment(f);
        register(d);
        return d;
    }

    public synchronized Deployment unregisterDeployedFile(File f) {
        FileDeployment d = new FileDeployment();
        d.setDeployedFile(f);
        return unregister(d);
    }

    public synchronized boolean isDeployed(File f) {
        FileDeployment d = new FileDeployment();
        d.setDeployedFile(f);
        return isRegistered(d);
    }

}
