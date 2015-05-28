package forklift.deployment;

import forklift.Registrar;

import java.io.File;
import java.io.IOException;

public class DeploymentManager extends Registrar<Deployment> {
    public synchronized Deployment registerDeployedFile(File f)
      throws IOException {
        final Deployment d = new Deployment(f);
        register(d);
        return d;
    }

    public synchronized Deployment unregisterDeployedFile(File f) {
        Deployment d = new Deployment();
        d.setDeployedFile(f);
        return unregister(d);
    }

    public synchronized boolean isDeployed(File f) {
        Deployment d = new Deployment();
        d.setDeployedFile(f);
        return isRegistered(d);
    }
}
