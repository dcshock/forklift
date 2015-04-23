package forklift.properties;

import forklift.deployment.Deployment;
import forklift.deployment.DeploymentEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;

public class PropertyDeploymentEvents implements DeploymentEvents {
    private static final Logger log = LoggerFactory.getLogger(PropertyDeploymentEvents.class);

    private static final Map<Deployment, Properties> deployments = new HashMap<>();

    @Override
    public void onDeploy(Deployment deployment) {
        log.info("Deploying: " + deployment);

        if (!deployment.getDeployedFile().getName().endsWith(".properties")) {
            log.warn("Invalid properties file.");
            return;
        }

        // Load and store properties.
        FileReader fr = null; 
        try {
            fr = new FileReader(deployment.getDeployedFile());

            final Properties props = new Properties();
            props.load(fr);

            synchronized (deployments) {
               deployments.put(deployment, props);
            }
        } catch (IOException e) {
            log.warn("File didn't exist while attempting to read.");
            return;
        } finally {
            try {
                if (fr != null)
                    fr.close();
            } catch (IOException ignored) {}
        }
    }

    @Override
    public void onUndeploy(Deployment deployment) {
        log.info("Undeploying: " + deployment);
        synchronized (deployments) {
            deployments.remove(deployment);
        }
    }

    /**
     * We allow jar/zip files.
     * @param  deployment 
     * @return            
     */
    @Override
    public boolean filter(Deployment deployment) {
        log.info("Filtering: " + deployment);

        return deployment.getDeployedFile().getName().endsWith(".properties");
    }

    public static Properties get(String name) {
        synchronized (deployments) {
            // This may be expensive someday, but let's go ahead, and keep it simple for now. 
            final Optional<Properties> props = deployments.keySet().stream()
                .filter(deployment -> deployment.getDeployedFile().getName().equals(name + ".properties"))
                .map(deployment -> deployments.get(deployment))
                .findFirst();

            if (props.isPresent())
                return props.get();

            return null;
        }
    }
}
