package forklift.consumer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {
    private AtomicBoolean running = new AtomicBoolean(false);
    private List<Class> consumerImpls = new ArrayList<Class>();
    
    private File deployedFile;
    
    public Consumer(File deployedFile) {
        this.deployedFile = deployedFile;
    }
}
