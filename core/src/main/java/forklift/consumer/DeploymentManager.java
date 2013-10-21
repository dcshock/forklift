package forklift.consumer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import forklift.connectors.ForkliftConnectorI;

public class DeploymentManager {
    private List<Consumer> consumers = new ArrayList<Consumer>();
    private ForkliftConnectorI connector;

    public DeploymentManager(ForkliftConnectorI connector) {
        this.connector = connector;
    }
    
    public synchronized Consumer registerDeployedFile(File f) {
        final Consumer c = new Consumer(f);
        
        
        return null;
    }
    
    public synchronized void unregisterDeployedFile(File f) {
        
    }
    
    public ForkliftConnectorI getConnector() {
        return connector;
    }
}
