package forklift;

import org.kohsuke.args4j.Option;

public class ForkliftOpts {
    @Option(name="-monitor1", required=true, usage="consumer deployment directory")
    private String consumerDir;

    @Option(name="-monitor2", usage="consumer deployment directory")
    private String propsDir;

    @Option(name="-url", required=true, usage="broker connection url")
    private String brokerUrl;

    @Option(name="-username", depends = "-password", usage = "username for broker connection")
    private String username;

    @Option(name="-password", depends = "-username", usage = "password for broker connection")
    private String password;

    @Option(name="-retryDir", usage="directory for persisted retry messages")
    private String retryDir;

    @Option(name="-retryESHost", usage="elastic search host name for retry storage")
    private String retryESHost;

    @Option(name="-retryESPort", usage="elastic search port number for retry storage")
    private int retryESPort = 9200;

    @Option(name="-retryESSsl", usage="connect to elastic search via ssl (https://)")
    private boolean retryESSsl;

    @Option(name="-runRetries", usage="run retries on this instance")
    private boolean runRetries;

    @Option(name="-replayDir", usage="replay log directory")
    private String replayDir;

    @Option(name="-replayESHost", usage="elastic search host name for replay storage")
    private String replayESHost;

    @Option(name="-replayESPort", usage="elastic search port number for replay storage")
    private int replayESPort = 9200;

    @Option(name="-replayESSsl", usage="connect to elastic search via ssl (https://)")
    private boolean replayESSsl;

    @Option(name="-replayESServer", usage="start an embedded elastic search server")
    private boolean replayESServer;

    @Option(name="-consulHost", usage="consul host name")
    private String consulHost = "localhost";

    @Option(name="-datadogApiKey", usage="Datadog API key if you want the lifecycle reported to datadog")
    private String datadogApiKey;

    @Option(name="-datadogApplicationKey", usage="Datadog application key if you for tracking at the app level")
    private String datadogApplicationKey;

    @Option(name="-config", usage="Config file for forklift server settings")
    private String configFileLocation;


    public String getConsumerDir() {
        return consumerDir;
    }

    public String getPropsDir() {
        return propsDir;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getRetryDir() {
        return retryDir;
    }

    public String getReplayDir() {
        return replayDir;
    }

    public String getConsulHost() {
        return consulHost;
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

    public boolean isRetryESSsl() {
        return retryESSsl;
    }

    public String getReplayESHost() {
        return replayESHost;
    }

    public int getReplayESPort() {
        return replayESPort;
    }

    public boolean isReplayESSsl() {
        return replayESSsl;
    }

    public boolean isReplayESServer() {
        return replayESServer;
    }

    public boolean isRunRetries() {
        return runRetries;
    }

    public String getDatadogApiKey() {
        return datadogApiKey;
    }

    public String getDatadogApplicationKey() {
        return datadogApplicationKey;
    }

    public String getConfigFileLocation() {
        return configFileLocation;
    }
}
