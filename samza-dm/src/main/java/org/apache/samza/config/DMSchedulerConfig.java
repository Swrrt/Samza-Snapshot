package org.apache.samza.config;

public class DMSchedulerConfig extends MapConfig {

    /**
     * The Scheduler class used.
     * Default: "org.apache.samza.job.dm.DefaultScheduler"
     */
    public static final String SCHEDULER_CLASS = "dm.scheduler.class";
    private static final String DEFAULT_SCHEDULER_CLASS = "org.apache.samza.job.dm.DefaultScheduler";
    public static final String SCHEDULER_LISTENER_CLASS = "dm.scheduler.listener.class";
    private static final String DEFAULT_SCHEDULER_LISTENER_CLASS = "org.apache.samza.job.dm.DMSchedulerListenerKafkaImpl";


    public DMSchedulerConfig(Config config) {
        super(config);
    }

    public String getSchedulerClass() { return get(SCHEDULER_CLASS, DEFAULT_SCHEDULER_CLASS); }

    public String getSchedulerListenerClass() { return get(SCHEDULER_LISTENER_CLASS, DEFAULT_SCHEDULER_LISTENER_CLASS); }

}
