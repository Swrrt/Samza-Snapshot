/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;

import java.util.*;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.apache.samza.PartitionChangeException;
import org.apache.samza.config.*;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.apache.samza.zk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A ClusterBasedJobCoordinator with StreamProcessor and ZooKeeper
 */
public class ClusterBasedApplicationMaster {

    private static final Logger log = LoggerFactory.getLogger(ClusterBasedApplicationMaster.class);

    private final Config config;

    private final ClusterManagerConfig clusterManagerConfig;

    /**
     * State to track container failures, host-container mappings
     */
    private final SamzaApplicationState state;

    //even though some of these can be converted to local variables, it will not be the case
    //as we add more methods to the JobCoordinator and completely implement SAMZA-881.

    /**
     * Handles callback for allocated containers, failed containers.
     */
    private final ScalingContainerProcessManager containerProcessManager;

    /**
     * A JobModelManager to return and refresh the {@link org.apache.samza.job.model.JobModel} when required.
     */
    private final JobModelManager jobModelManager;

    /*
     * The interval for polling the Task Manager for shutdown.
     */
    private final long jobCoordinatorSleepInterval;

    /*
     * Config specifies if a Jmx server should be started on this Job Coordinator
     */
    private final boolean isJmxEnabled;

    /**
     * Internal boolean to check if the job coordinator has already been started.
     */
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * A boolean variable indicating whether the job has durable state stores in the configuration
     */
    private final boolean hasDurableStores;

    /**
     * The input topic partition count monitor
     */
    private final StreamPartitionCountMonitor partitionMonitor;

    /**
     * Metrics to track stats around container failures, needed containers etc.
     */
    private final MetricsRegistryMap metrics;

    /**
     * Internal variable for the instance of {@link JmxServer}
     */
    private JmxServer jmxServer;

    /**
     * Variable to keep the callback exception
     */
    volatile private Exception coordinatorException = null;
    /*
     * For ZooKeeper
     */
    private ScheduleAfterDebounceTime debounceTimer = null;
    private int debounceTimeMs;
    private static final String DEFAULT_JOB_ID = "1";
    private static final String DEFAULT_JOB_NAME = "defaultJob";
    private static final String JOB_COORDINATOR_ZK_PATH_FORMAT = "%s/%s-%s-coordinationData";
    private final LeaderZkJobCoordinator leaderZkJobCoordinator;
    private Map<TaskName, Integer> changeLogPartitionMap = new HashMap<>();
    private StreamMetadataCache streamMetadata = null;
    private JobModel jobModel = null;
    /**
     *
     * Creates a new ClusterBasedJobCoordinator instance from a config. Invoke run() to actually
     * run the jobcoordinator.
     *
     * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
     *                                {@link org.apache.samza.job.model.JobModel} from.
     */
    public ClusterBasedApplicationMaster(Config coordinatorSystemConfig) {

        metrics = new MetricsRegistryMap();

        //build a JobModelReader and perform partition assignments.
        jobModelManager = buildJobModelManager(coordinatorSystemConfig, metrics);
        config = jobModelManager.jobModel().getConfig();
        hasDurableStores = new StorageConfig(config).hasDurableStores();
        state = new SamzaApplicationState(jobModelManager);

        partitionMonitor = getPartitionCountMonitor(config);
        clusterManagerConfig = new ClusterManagerConfig(config);
        isJmxEnabled = clusterManagerConfig.getJmxEnabled();

        jobCoordinatorSleepInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();
        leaderZkJobCoordinator = new LeaderZkJobCoordinator(config, metrics, getZkUtils(config,metrics));
        // build a container process Manager

        containerProcessManager = new ScalingContainerProcessManager(config, state, metrics);

    }
    private ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry) {
        ZkConfig zkConfig = new ZkConfig(config);
        ZkKeyBuilder keyBuilder = new ZkKeyBuilder(getJobCoordinationZkPath(config));
        ZkClient zkClient = ZkCoordinationUtilsFactory
                .createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
        return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), metricsRegistry);
    }
    public static String getJobCoordinationZkPath(Config config) {
        JobConfig jobConfig = new JobConfig(config);
        String appId = new ApplicationConfig(config).getGlobalAppId();
        String jobName = jobConfig.getName().isDefined()
                ? jobConfig.getName().get()
                : DEFAULT_JOB_NAME;
        String jobId = jobConfig.getJobId().isDefined()
                ? jobConfig.getJobId().get()
                : DEFAULT_JOB_ID;
        return String.format(JOB_COORDINATOR_ZK_PATH_FORMAT, appId, jobName, jobId);
    }

    /**
     * Starts the JobCoordinator.
     *
     */
    public void run() {
        if (!isStarted.compareAndSet(false, true)) {
            log.info("Attempting to start an already started AM. ");
            return;
        }
        // set up JmxServer (if jmx is enabled)
        if (isJmxEnabled) {
            jmxServer = new JmxServer();
            state.jmxUrl = jmxServer.getJmxUrl();
            state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
        } else {
            jmxServer = null;
        }

        try {
            //initialize JobCoordinator state
            log.info("Starting ClusterBasedApplicationMaster");
            leaderZkJobCoordinator.start(jobModelManager.jobModel());
            containerProcessManager.start();
            partitionMonitor.start();

            boolean isInterrupted = false;
            int counter = 0;
            jobModel = jobModelManager.jobModel();
            while (!containerProcessManager.shouldShutdown() && !checkAndThrowException() && !isInterrupted) {
                try {
                    counter++;
                    Thread.sleep(jobCoordinatorSleepInterval);
                    if(counter == 120){
                        counter = 0;
                        leaderZkJobCoordinator.publishJobModel(scaleUpByOne());
                    }
                } catch (InterruptedException e) {
                    isInterrupted = true;
                    log.error("Interrupted in AM loop {} ", e);
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Throwable e) {
            log.error("Exception thrown in AM loop {} ", e);
            throw new SamzaException(e);
        } finally {
            onShutDown();
        }
    }
    private JobModel scaleUpByOne(){
        List<String> processors = new ArrayList<>(jobModel.getContainers().keySet());
        processors.add(processors.get(0).concat("_"+processors.size()));
        jobModel = generateNewJobModel(processors);
        return jobModel;
    }
    private JobModel generateNewJobModel(List<String> processors) {
        for (ContainerModel containerModel : jobModel.getContainers().values()) {
            containerModel.getTasks().forEach((taskName, taskModel) -> changeLogPartitionMap.put(taskName, taskModel.getChangelogPartition().getPartitionId()));
        }
        return JobModelManager.readJobModel(this.config, changeLogPartitionMap, null, streamMetadata, processors);
    }
    private boolean checkAndThrowException() throws Exception {
        if (coordinatorException != null) {
            throw coordinatorException;
        }
        return false;
    }

    /**
     * Stops all components of the JobCoordinator.
     */
    private void onShutDown() {

        try {
            partitionMonitor.stop();
            containerProcessManager.stop();
        } catch (Throwable e) {
            log.error("Exception while stopping task manager {}", e);
        }
        log.info("Stopped task manager");

        if (jmxServer != null) {
            try {
                jmxServer.stop();
                log.info("Stopped Jmx Server");
            } catch (Throwable e) {
                log.error("Exception while stopping jmx server {}", e);
            }
        }
    }

    private JobModelManager buildJobModelManager(Config coordinatorSystemConfig, MetricsRegistryMap registry)  {
        JobModelManager jobModelManager = JobModelManager.apply(coordinatorSystemConfig, registry);
        return jobModelManager;
    }

    private StreamPartitionCountMonitor getPartitionCountMonitor(Config config) {
        Map<String, SystemAdmin> systemAdmins = new JavaSystemConfig(config).getSystemAdmins();
        streamMetadata = new StreamMetadataCache(Util.javaMapAsScalaMap(systemAdmins), 0, SystemClock.instance());
        Set<SystemStream> inputStreamsToMonitor = new TaskConfigJava(config).getAllInputStreams();
        if (inputStreamsToMonitor.isEmpty()) {
            throw new SamzaException("Input streams to a job can not be empty.");
        }

        return new StreamPartitionCountMonitor(
                inputStreamsToMonitor,
                streamMetadata,
                metrics,
                new JobConfig(config).getMonitorPartitionChangeFrequency(),
                streamsChanged -> {
                    // Fail the jobs with durable state store. Otherwise, application state.status remains UNDEFINED s.t. YARN job will be restarted
                    if (hasDurableStores) {
                        log.error("Input topic partition count changed in a job with durable state. Failing the job.");
                        state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
                    }
                    coordinatorException = new PartitionChangeException("Input topic partition count changes detected.");
                });
    }

    // The following two methods are package-private and for testing only
    @VisibleForTesting
    SamzaApplicationState.SamzaAppStatus getAppStatus() {
        // make sure to only return a unmodifiable copy of the status variable
        final SamzaApplicationState.SamzaAppStatus copy = state.status;
        return copy;
    }

    @VisibleForTesting
    StreamPartitionCountMonitor getPartitionMonitor() {
        return partitionMonitor;
    }


    /**
     * The entry point for the {@link ClusterBasedApplicationMaster}
     * @param args args
     */
    public static void main(String[] args) {
        Config coordinatorSystemConfig = null;
        log.info("Using LeaderZkJobCoordinator now");
        final String coordinatorSystemEnv = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG());
        try {
            //Read and parse the coordinator system config.
            log.info("Parsing coordinator system config {}", coordinatorSystemEnv);
            coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(coordinatorSystemEnv, Config.class));
        } catch (IOException e) {
            log.error("Exception while reading coordinator stream config {}", e);
            throw new SamzaException(e);
        }
        ClusterBasedApplicationMaster jc = new ClusterBasedApplicationMaster(coordinatorSystemConfig);
        jc.run();
    }
}
