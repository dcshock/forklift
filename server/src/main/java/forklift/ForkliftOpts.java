package forklift;

import org.kohsuke.args4j.Option;

public class ForkliftOpts {
    @Option(name="-monitor1", required=true, usage="consumer deployment directory")
    private String consumerDir;

    @Option(name="-monitor2", usage="consumer deployment directory")
    private String propsDir;

    @Option(name="-url", required=true, usage="broker connection url")
    private String brokerUrl;

    @Option(name="-retryDir", usage="directory for persisted retry messages")
    private String retryDir;

    @Option(name="-replayDir", usage="replay log directory")
    private String replayDir;

    public String getConsumerDir() {
        return consumerDir;
    }

    public void setConsumerDir(String consumerDir) {
        this.consumerDir = consumerDir;
    }

    public String getPropsDir() {
        return propsDir;
    }

    public void setPropsDir(String propsDir) {
        this.propsDir = propsDir;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getRetryDir() {
        return retryDir;
    }

    public void setRetryDir(String retryDir) {
        this.retryDir = retryDir;
    }

    public String getReplayDir() {
        return replayDir;
    }

    public void setReplayDir(String replayDir) {
        this.replayDir = replayDir;
    }
}
