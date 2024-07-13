package mg.lahatra3.beans;

import java.util.Objects;

public class SparkConfiguration {

    private final String appName;
    private final String masterUrl;
    private final String executor;
    private final String core;
    private final String memory;
    private final String extraJavaOptions;

    /**
     * 
     * Constructs a new {@code SparkConfiguration} instance.
     * 
     * @param appName appName Name of spark job
     * @param masterUrl masterUrl Url of spark cluster
     * @param executor executor Number of executor when processing data
     * @param core core Number of CPUs core used by spark
     * @param memory memory Resource allocated
     * @param extraJavaOptions
     * 
     */

    public SparkConfiguration(
        String appName,
        String masterUrl,
        String executor,
        String core,
        String memory,
        String extraJavaOptions
    ) {
        Objects.requireNonNull(appName, "appName cannot be null ...");
        Objects.requireNonNull(masterUrl, "masterUrl cannot be null ...");
        Objects.requireNonNull(executor, "executor cannot be null ...");
        Objects.requireNonNull(core, "core cannot be null ...");
        Objects.requireNonNull(memory, "memory cannot be null ...");

        this.appName = appName;
        this.masterUrl = masterUrl;
        this.executor = executor;
        this.core = core;
        this.memory = memory;
        this.extraJavaOptions = extraJavaOptions;
    }

    // Getter methods
    public String getAppName() {
        return appName;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public String getExecutor() {
        return executor;
    }

    public String getCore() {
        return core;
    }

    public String getMemory() {
        return memory;
    }

    public String getExtraJavaOptions() {
        return extraJavaOptions;
    }
}
