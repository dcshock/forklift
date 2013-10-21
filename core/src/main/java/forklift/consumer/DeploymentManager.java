package forklift.consumer;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import forklift.connectors.ForkliftConnectorI;

public class DeploymentManager {
    private AtomicLong consumerIndex;
    private ForkliftConnectorI connector;

    public DeploymentManager(ForkliftConnectorI connector) {
        this.connector = connector;
        
        consumerIndex = new AtomicLong(0);
    }
    
    public Long registerConsumer(File f) {
        final Long id = consumerIndex.getAndIncrement();
        return id;
    }
    
    public void destroyConsumer(Object id) {
        
    }
    
    public ForkliftConnectorI getConnector() {
        return connector;
    }
}
