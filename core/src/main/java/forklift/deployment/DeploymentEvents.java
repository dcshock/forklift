package forklift.deployment;

public interface DeploymentEvents {
    void onDeploy(Deployment deployment);
    void onUndeploy(Deployment deployment);

    /**
     * Process this deployment?
     * @param  deployment [description]
     * @return            [description]
     */
    default boolean filter(Deployment deployment) {
        return true;
    }
}
