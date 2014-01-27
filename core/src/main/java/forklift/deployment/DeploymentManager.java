package forklift.deployment;

import java.io.File;
import java.net.MalformedURLException;

import forklift.Registrar;

public class DeploymentManager extends Registrar<Deployment> {
    public synchronized Deployment registerDeployedFile(File f) 
      throws MalformedURLException {
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
