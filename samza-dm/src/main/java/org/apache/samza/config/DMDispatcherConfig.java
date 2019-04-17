package org.apache.samza.config;

public class DMDispatcherConfig extends MapConfig {

    /**
     * The Scheduler class used.
     * Default: "org.apache.samza.job.dm.DMJobFactory"
     */
    public static final String DISPATCHER_CLASS = "dm.dispatcher.class";
    private static final String DEFAULT_DISPATCHER_CLASS = "org.apache.samza.job.dm.DefaultDispatcher";


    public DMDispatcherConfig(Config config) {
        super(config);
    }

    public String getDispatcherClass() { return get(DISPATCHER_CLASS, DEFAULT_DISPATCHER_CLASS); }
}
