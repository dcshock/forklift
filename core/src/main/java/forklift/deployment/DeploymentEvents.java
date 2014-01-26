package forklift.deployment;

public interface DeploymentEvents {
    void onDeploy(Deployment deployment);
    void onUndeploy(Deployment deployment);
}
