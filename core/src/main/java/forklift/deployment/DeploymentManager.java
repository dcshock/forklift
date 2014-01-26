package forklift.deployment;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DeploymentManager {
    private List<Deployment> deployments = new ArrayList<Deployment>();

    public synchronized Deployment registerDeployedFile(File f) 
      throws MalformedURLException {
        final Deployment d = new Deployment(f);
        deployments.add(d);
        return d;
    }
    
    public synchronized Deployment unregisterDeployedFile(File f) {
        Iterator<Deployment> it = deployments.iterator();
        while (it.hasNext()) {
            Deployment d = it.next();
            if (d.getDeployedFile().equals(f)) { 
                it.remove();
                return d;
            }
        }
        return null;
    }
    
    public synchronized boolean isDeployed(File f) {
        Iterator<Deployment> it = deployments.iterator();
        while (it.hasNext()) {
            Deployment c = it.next();
            if (c.getDeployedFile().equals(f)) 
                return true;
        }
        return false;
    }
}
