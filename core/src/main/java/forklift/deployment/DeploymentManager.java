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
        final Deployment c = new Deployment(f);
        deployments.add(c);
        return c;
    }
    
    public synchronized void unregisterDeployedFile(File f) {
        Iterator<Deployment> it = deployments.iterator();
        while (it.hasNext()) {
            Deployment c = it.next();
            if (c.getDeployedFile().equals(f)) 
                it.remove();
        }
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
