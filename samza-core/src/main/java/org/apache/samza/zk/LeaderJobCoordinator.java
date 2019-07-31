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
package org.apache.samza.zk;

import com.google.common.annotations.VisibleForTesting;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.runtime.ProcessorIdGenerator;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.util.MetricsReporterLoader;
//import org.apache.samza.job.dm.MixedLoadBalancer.MixedLoadBalanceManager;
import org.apache.samza.zk.RMI.MetricsRetrieverRMIClient;
import org.apache.samza.zk.RMI.MetricsServer;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class LeaderJobCoordinator implements ZkControllerListener, JobCoordinator{
    private static final Logger LOG = LoggerFactory.getLogger(LeaderJobCoordinator.class);
    // TODO: MetadataCache timeout has to be 0 for the leader so that it can always have the latest information associated
    // with locality. Since host-affinity is not yet implemented, this can be fixed as part of SAMZA-1197
    private static final int METADATA_CACHE_TTL_MS = 5000;
    private static final int NUM_VERSIONS_TO_LEAVE = 10;
    // Action name when the JobModel version changes
    private static final String JOB_MODEL_VERSION_CHANGE = "JobModelVersionChange";

    // Action name when the Processor membership changes
    private static final String ON_PROCESSOR_CHANGE = "OnProcessorChange";

    /**
     * Cleanup process is started after every new job model generation is complete.
     * It deletes old versions of job model and the barrier.
     * How many to delete (or to leave) is controlled by @see org.apache.samza.zk.ZkJobCoordinator#NUM_VERSIONS_TO_LEAVE.
     **/
    private static final String ON_ZK_CLEANUP = "OnCleanUp";

    private final ZkUtils zkUtils;
    private final String processorId;
    private final ZkController zkController;

    private final Config config;
    private final ZkBarrierForVersionUpgrade barrier;
    private final ZkJobCoordinatorMetrics metrics;
    private final Map<String, MetricsReporter> reporters;
    //private MixedLoadBalanceManager mixedLoadBalanceManager;
    private StreamMetadataCache streamMetadataCache = null;
    private ScheduleAfterDebounceTime debounceTimer = null;
    private JobModel newJobModel = null;
    private List<String> currentProcessors = null;
    private int debounceTimeMs;
    private boolean hasCreatedChangeLogStreams = false;
    private String cachedJobModelVersion = null;
    private Map<TaskName, Integer> changeLogPartitionMap = new HashMap<>();
    private Map<String, String> containerToProcessorMap = null;
    private MetricsRetrieverRMIClient rmiClient = null;
    public LeaderJobCoordinator(Config config, MetricsRegistry metricsRegistry, ZkUtils zkUtils, JobModel jobModel) {
        this.config = config;
        //this.mixedLoadBalanceManager = new MixedLoadBalanceManager();
        this.metrics = new ZkJobCoordinatorMetrics(metricsRegistry);
        this.processorId = createProcessorId(config);
        this.zkUtils = zkUtils;
        // setup a listener for a session state change
        // we are mostly interested in "session closed" and "new session created" events
        zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
        this.zkController = new LeaderZkControllerImpl(processorId, zkUtils, this);
        this.barrier =  new ZkBarrierForVersionUpgrade(
                zkUtils.getKeyBuilder().getJobModelVersionBarrierPrefix(),
                zkUtils,
                new ZkBarrierListenerImpl());
        this.debounceTimeMs = new JobConfig(config).getDebounceTimeMs();
        this.reporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), processorId);
        //
        newJobModel = jobModel;
        currentProcessors = new LinkedList<>();
        debounceTimer = new ScheduleAfterDebounceTime();
        debounceTimer.setScheduledTaskCallback(throwable -> {
            LOG.error("Received exception from in JobCoordinator Processing!", throwable);
            stop();
        });
        if(config.getBoolean("job.loadbalance.on", false)) {
            this.rmiClient = new MetricsRetrieverRMIClient(
                    config.get("job.loadbalance.dm.address",""),
                    Integer.parseInt(config.get("job.loadbalance.dm.rmiport","8881")),
                    config.get("job.default.system"),
                    config.get("job.loadbalance.inputtopic")
            );
        }
    }

    @Override
    public void start(){
        LOG.info("Leader JobCoordinator start");
        startMetrics();
        streamMetadataCache = StreamMetadataCache.apply(METADATA_CACHE_TTL_MS, config);
        zkController.register();
        if(config.getBoolean("job.loadbalance.on", false)) {
            rmiClient.sendAddress("Leader", getHostName());
        }
        //mixedLoadBalanceManager.initial(newJobModel, config);
    }
    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Failed to fetch hostname of the leader", e);
            throw new SamzaException(e);
        }
    }
    @Override
    public JobModel getJobModel(){
        return newJobModel;
    };

    @Override
    public String getProcessorId(){
        return processorId;
    };
    @Override
    public void setListener(JobCoordinatorListener listener){
    };
    public synchronized void stop() {
        //Setting the isLeader metric to false when the stream processor shuts down because it does not remain the leader anymore
        metrics.isLeader.set(false);
        debounceTimer.stopScheduler();
        zkController.stop();

        shutdownMetrics();
    }

    private void startMetrics() {
        for (MetricsReporter reporter: reporters.values()) {
            reporter.register("job-coordinator-" + processorId, (ReadableMetricsRegistry) metrics.getMetricsRegistry());
            reporter.start();
        }
    }

    private void shutdownMetrics() {
        for (MetricsReporter reporter: reporters.values()) {
            reporter.stop();
        }
    }
    //////////////////////////////////////////////// LEADER stuff ///////////////////////////
    @Override
    public void onProcessorChange(List<String> processors) {
        LOG.info("Leader JobCoordinator::onProcessorChange - list of processors changed! List size=" + processors.size());
        if(processors != null && processors.size() > 0) currentProcessors = processors;
        debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs,
                () -> doOnProcessorChange(processors));
    }
    //Use this to publish jobModel without timeout
    public void onJobModelChange(){
        doOnProcessorChange(currentProcessors);
    }

    void doOnProcessorChange(List<String> processors) {
        // if list of processors is empty - it means we are called from 'onBecomeLeader'
        // TODO: Handle empty currentProcessorIds.
        if(newJobModel == null){
            LOG.warn("JobModel is not ready yet");
            return ;
        }
        JobModel jobModel = newJobModel;
        //TODO: Change this to our mechanism
        if(processors != null && processors.size() == jobModel.getContainers().size()){ //Equal or scale in
            List<String> currentProcessorIds = getActualProcessorIds(processors);
            Set<String> uniqueProcessorIds = new HashSet<String>(currentProcessorIds);
            if (currentProcessorIds.size() != uniqueProcessorIds.size()) {
                LOG.info("Processors: {} has duplicates. Not generating job model.", currentProcessorIds);
                return;
            }

            /*if (!jobModel.getContainers().keySet().contains(currentProcessorIds.get(0))){
                /* Remapping the ProcessorsID */
            /*    LOG.info("Need to map processorID to job model");
                if(containerToProcessorMap == null){
                    containerToProcessorMap = new HashMap<String, String>();
                }
                Set <String> notUsedProcessors = new HashSet(currentProcessorIds);
                for (String containerID: jobModel.getContainers().keySet()){
                    if(currentProcessorIds.contains(containerToProcessorMap.get(containerID))){
                        notUsedProcessors.remove(containerToProcessorMap.get(containerID));
                    }else{
                        containerToProcessorMap.remove(containerID);
                    }
                }
                for (String containerID: jobModel.getContainers().keySet()){
                    if(containerToProcessorMap.get(containerID)==null){
                        String x = notUsedProcessors.iterator().next();
                        containerToProcessorMap.put(containerID, x);
                        notUsedProcessors.remove(x);
                    }
                }
                Map<String, ContainerModel> models = new HashMap<>();
                for(ContainerModel container: jobModel.getContainers().values()){
                    models.put(containerToProcessorMap.get(container.getProcessorId()),new ContainerModel(containerToProcessorMap.get(container.getProcessorId()),container.getContainerId(),container.getTasks()));
                }
                jobModel = new JobModel(jobModel.getConfig(),models);
                LOG.info("JobModel after remapping: " + jobModel);
            }*/

            if (!hasCreatedChangeLogStreams) {
                JobModelManager.createChangeLogStreams(new StorageConfig(config), jobModel.maxChangeLogStreamPartitions);
                hasCreatedChangeLogStreams = true;
            }
            // Assign the next version of JobModel
            String currentJMVersion = zkUtils.getJobModelVersion();
            String nextJMVersion;
            if (currentJMVersion == null) {
                nextJMVersion = "1";
            } else {
                nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
            }
            LOG.info("Leader received new Job Model. Version = " + nextJMVersion);
            // Publish the new job model
            zkUtils.publishJobModel(nextJMVersion, jobModel);

            // Start the barrier for the job model update
            barrier.create(nextJMVersion, currentProcessorIds);

            // Notify all processors about the new JobModel by updating JobModel Version number
            zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

            LOG.info("Leader Published new Job Model. Version = " + nextJMVersion);

            //LOG.info("Job Model: " + jobModel);
            debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
        }else if(processors != null && processors.size() > jobModel.getContainers().size()) {
            //Scale in
            LOG.info("Need to scale in");
            List<String> currentProcessorIds = getActualProcessorIds(processors);
            List<String> newProcessorId = new ArrayList<>();
            for(String processor: currentProcessorIds){
                if(jobModel.getContainers().containsKey(processor))newProcessorId.add(processor);
            }
            if (!hasCreatedChangeLogStreams) {
                JobModelManager.createChangeLogStreams(new StorageConfig(config), jobModel.maxChangeLogStreamPartitions);
                hasCreatedChangeLogStreams = true;
            }
            // Assign the next version of JobModel
            String currentJMVersion = zkUtils.getJobModelVersion();
            String nextJMVersion;
            if (currentJMVersion == null) {
                nextJMVersion = "1";
            } else {
                nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
            }
            LOG.info("Leader received new Job Model. Version = " + nextJMVersion);
            // Publish the new job model
            zkUtils.publishJobModel(nextJMVersion, jobModel);

            // Start the barrier for the job model update
            LOG.info("Only need to wait for confirmation from: " + newProcessorId);
            barrier.create(nextJMVersion, newProcessorId);

            // Notify all processors about the new JobModel by updating JobModel Version number
            zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

            LOG.info("Leader Published new Job Model. Version = " + nextJMVersion);

            //LOG.info("Job Model: " + jobModel);
            debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
        }else{
            LOG.info("Need to wait for all Processors online to pulish new JobModel!");
        }
    }

    public void forcePublishJobModel(JobModel jobModel){
        String currentJMVersion = zkUtils.getJobModelVersion();
        String nextJMVersion;
        if (currentJMVersion == null) {
            nextJMVersion = "1";
        } else {
            nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
        }
        LOG.info("Leader generated new Job Model. Version = " + nextJMVersion);
        // Publish the new job model
        zkUtils.publishJobModel(nextJMVersion, jobModel);

        List<String> a = new ArrayList<>(jobModel.getContainers().keySet());
        // Start the barrier for the job model update
        barrier.create(nextJMVersion, a);

        // Notify all processors about the new JobModel by updating JobModel Version number
        zkUtils.publishJobModelVersion(currentJMVersion, nextJMVersion);

        LOG.info("Leader force Published new Job Model. Version = " + nextJMVersion);

        //LOG.info("Job Model: " + jobModel);
        debounceTimer.scheduleAfterDebounceTime(ON_ZK_CLEANUP, 0, () -> zkUtils.cleanupZK(NUM_VERSIONS_TO_LEAVE));
    }

    @Override
    public void onNewJobModelAvailable(final String version) {
    }

    @Override
    public void onNewJobModelConfirmed(String version) {
    }
    public List<String> getCurrentProcessors(){
        return currentProcessors;
    }
    private String createProcessorId(Config config) {
        // TODO: This check to be removed after 0.13+
        ApplicationConfig appConfig = new ApplicationConfig(config);
        if (appConfig.getProcessorId() != null) {
            return appConfig.getProcessorId();
        } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
            ProcessorIdGenerator idGenerator =
                    ClassLoaderHelper.fromClassName(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class);
            return idGenerator.generateProcessorId(config);
        } else {
            throw new ConfigException(String
                    .format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
                            ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
        }
    }

    private List<String> getActualProcessorIds(List<String> processors) {
        if (processors.size() > 0) {
            // we should use this list
            // but it needs to be converted into PIDs, which is part of the data
            return zkUtils.getActiveProcessorsIDs(processors);
        } else {
            // get the current list of processors
            return zkUtils.getSortedActiveProcessorsIDs();
        }
    }

    /**
     * Generate new JobModel when becoming a leader or the list of processor changed.
     */
    private JobModel generateNewJobModel(List<String> processors) {
        // If JobModel exists in zookeeper && cached JobModel version is unequal to JobModel version stored in zookeeper.
        JobModel jobModel = newJobModel;
        for (ContainerModel containerModel : jobModel.getContainers().values()) {
            containerModel.getTasks().forEach((taskName, taskModel) -> changeLogPartitionMap.put(taskName, taskModel.getChangelogPartition().getPartitionId()));
        }

        /**
         * Host affinity is not supported in standalone. Hence, LocalityManager(which is responsible for container
         * to host mapping) is passed in as null when building the jobModel.
         */
        return JobModelManager.readJobModel(this.config, changeLogPartitionMap, null, streamMetadataCache, processors);
    }

    class ZkBarrierListenerImpl implements ZkBarrierListener {
        private final String barrierAction = "BarrierAction";

        private long startTime = 0;

        @Override
        public void onBarrierCreated(String version) {
            // Start the timer for rebalancing
            startTime = System.nanoTime();

            metrics.barrierCreation.inc();
            debounceTimer.scheduleAfterDebounceTime(
                    barrierAction,
                    (new ZkConfig(config)).getZkBarrierTimeoutMs(),
                    () -> barrier.expire(version)
            );
        }

        public void onBarrierStateChanged(final String version, ZkBarrierForVersionUpgrade.State state) {
            LOG.info("JobModel version " + version + " obtained consensus successfully!");
            metrics.barrierStateChange.inc();
            metrics.singleBarrierRebalancingTime.update(System.nanoTime() - startTime);
            if (ZkBarrierForVersionUpgrade.State.DONE.equals(state)) {
                debounceTimer.scheduleAfterDebounceTime(barrierAction, 0, () -> onNewJobModelConfirmed(version));
            } else {
                if (ZkBarrierForVersionUpgrade.State.TIMED_OUT.equals(state)) {
                    // no-op for non-leaders
                    // for leader: make sure we do not stop - so generate a new job model
                    LOG.warn("Barrier for version " + version + " timed out.");
                    if (zkController.isLeader()) {
                        LOG.info("Leader will schedule a new job model generation");
                        debounceTimer.scheduleAfterDebounceTime(ON_PROCESSOR_CHANGE, debounceTimeMs, () ->
                        {
                            // actual actions to do are the same as onProcessorChange
                            doOnProcessorChange(new ArrayList<>());
                        });
                    }
                }
            }
        }

        @Override
        public void onBarrierError(String version, Throwable t) {
            LOG.error("Encountered error while attaining consensus on JobModel version " + version);
            metrics.barrierError.inc();
            stop();
        }
    }

    /// listener to handle session expiration
    class ZkSessionStateChangedListener implements IZkStateListener {

        private static final String ZK_SESSION_ERROR = "ZK_SESSION_ERROR";

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState state)
                throws Exception {
            if (state == Watcher.Event.KeeperState.Expired) {
                // if the session has expired it means that all the registration's ephemeral nodes are gone.
                LOG.warn("Got session expired event for processor=" + processorId);

                // increase generation of the ZK connection. All the callbacks from the previous generation will be ignored.
                zkUtils.incGeneration();

                // reset all the values that might have been from the previous session (e.g ephemeral node path)
                zkUtils.unregister();

            }
        }

        @Override
        public void handleNewSession()
                throws Exception {
            LOG.info("Got new session created event for processor=" + processorId);

            LOG.info("register zk controller for the new session");
            zkController.register();
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error)
                throws Exception {
            // this means we cannot connect to zookeeper
            LOG.info("handleSessionEstablishmentError received for processor=" + processorId, error);
            debounceTimer.scheduleAfterDebounceTime(ZK_SESSION_ERROR, 0, () -> stop());
        }
    }
    /* For testing */
    public void publishJobModel(JobModel jobModel){
        newJobModel = jobModel;
        LOG.info("New JobModel comes into Leader!");
        onJobModelChange();
    }
    public JobModel testingGenerateNewJobModel(List<String> processors){
        // Generate new Job Model using MixedLoadBalanceManager
        //return mixedLoadBalanceManager.generateNewJobModel(processors);
        return generateNewJobModel(processors);
    }
    public JobModel testingRebalance(){
        return null;
        //return mixedLoadBalanceManager.rebalanceJobModel();
    }
    @VisibleForTesting
    public ZkUtils getZkUtils() {
        return zkUtils;
    }
}
