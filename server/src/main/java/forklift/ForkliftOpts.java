package forklift;

import org.kohsuke.args4j.Option;

public class ForkliftOpts {
    @Option(name = "-monitor1", required = true, usage = "consumer deployment directory")
    private String consumerDir;

    @Option(name = "-monitor2", usage = "consumer deployment directory")
    private String propsDir;

    @Option(name = "-url", required = false, usage = "specify the application name for this instance")
    private String applicationName;

    @Option(name = "-url", required = true, usage = "broker connection url")
    private String brokerUrl;

    @Option(name = "-retryDir", usage = "directory for persisted retry messages")
    private String retryDir;

    @Option(name = "-retryESHost", usage = "elastic search host name for retry storage")
    private String retryESHost;

    @Option(name = "-retryESPort", usage = "elastic search port number for retry storage")
    private int retryESPort = 9200;

    @Option(name = "-retryESSsl", usage = "connect to elastic search via ssl (https://)")
    private boolean retryESSsl;

    @Option(name = "-runRetries", usage = "run retries on this instance")
    private boolean runRetries;

    @Option(name = "-replayDir", usage = "replay log directory")
    private String replayDir;

    @Option(name = "-replayESHost", usage = "elastic search host name for replay storage")
    private String replayESHost;

    @Option(name = "-replayESPort", usage = "elastic search port number for replay storage")
    private int replayESPort = 9200;

    @Option(name = "-replayESSsl", usage = "connect to elastic search via ssl (https://)")
    private boolean replayESSsl;

    @Option(name = "-replayESServer", usage = "start an embedded elastic search server")
    private boolean replayESServer;

    @Option(name = "-consulHost", usage = "consul host name")
    private String consulHost = "localhost";

    @Option(name = "-replayESCluster", usage = "name of the elastic search cluster to use for replay logs.")
    private String replayESCluster = "elasticsearch";

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

    public String getConsulHost() {
        return consulHost;
    }

    public void setConsulHost(String consulHost) {
        this.consulHost = consulHost;
    }

    public String getRetryESHost() {
        return retryESHost;
    }

    public void setRetryESHost(String retryESHost) {
        this.retryESHost = retryESHost;
    }

    public int getRetryESPort() {
        return retryESPort;
    }

    public void setRetryESPort(int retryESPort) {
        this.retryESPort = retryESPort;
    }

    public boolean isRetryESSsl() {
        return retryESSsl;
    }

    public void setRetryESSsl(boolean retryESSsl) {
        this.retryESSsl = retryESSsl;
    }

    public String getReplayESHost() {
        return replayESHost;
    }

    public void setReplayESHost(String replayESHost) {
        this.replayESHost = replayESHost;
    }

    public int getReplayESPort() {
        return replayESPort;
    }

    public void setReplayESPort(int replayESPort) {
        this.replayESPort = replayESPort;
    }

    public boolean isReplayESSsl() {
        return replayESSsl;
    }

    public void setReplayESSsl(boolean replayESSsl) {
        this.replayESSsl = replayESSsl;
    }

    public boolean isReplayESServer() {
        return replayESServer;
    }

    public void setReplayESServer(boolean replayESServer) {
        this.replayESServer = replayESServer;
    }

    public void setRunRetries(boolean runRetries) {
        this.runRetries = runRetries;
    }

    public boolean isRunRetries() {
        return runRetries;
    }

    public String getReplayESCluster() {
        return replayESCluster;
    }

    public void setReplayESCluster(String replayESCluster) {
        this.replayESCluster = replayESCluster;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }
}
