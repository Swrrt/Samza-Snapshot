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

package org.apache.samza.runtime;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.MDC;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerExceptionHandler;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ScalaToJavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalContainerRunner is the local runner for Yarn {@link SamzaContainer}s. It is an intermediate step to
 * have a local runner for yarn before we consolidate the Yarn container and coordination into a
 * {@link org.apache.samza.processor.StreamProcessor}. This class will be replaced by the {@link org.apache.samza.processor.StreamProcessor}
 * local runner once that's done.
 *
 * Since we don't have the {@link org.apache.samza.coordinator.JobCoordinator} implementation in Yarn, the components (jobModel and containerId)
 * are directly inside the runner.
 */
public class LocalStreamProcessorRunner extends AbstractApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(LocalStreamProcessorRunner.class);
    private final JobModel jobModel;
    private final String containerId;
    private final Set<StreamProcessor> processors = ConcurrentHashMap.newKeySet();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicInteger numProcessorsToStart = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private ApplicationStatus appStatus = ApplicationStatus.New;
    public LocalStreamProcessorRunner(JobModel jobModel, String containerId) {
        super(jobModel.getConfig());
        this.jobModel = jobModel;
        this.containerId = containerId;
    }
    final class FollowerStreamProcessorLifeCycleListener implements StreamProcessorLifecycleListener {
        StreamProcessor processor;

        void setProcessor(StreamProcessor processor) {
            this.processor = processor;
        }

        @Override
        public void onStart() {
            if (numProcessorsToStart.decrementAndGet() == 0) {
                appStatus = ApplicationStatus.Running;
            }
        }

        @Override
        public void onShutdown() {
            processors.remove(processor);
            processor = null;

            if (processors.isEmpty()) {
                shutdownAndNotify();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            processors.remove(processor);
            processor = null;

            if (failure.compareAndSet(null, t)) {
                // shutdown the other processors
                processors.forEach(StreamProcessor::stop);
            }

            if (processors.isEmpty()) {
                shutdownAndNotify();
            }
        }
        private void shutdownAndNotify() {
            if (failure.get() != null) {
                appStatus = ApplicationStatus.unsuccessfulFinish(failure.get());
            } else {
                if (appStatus == ApplicationStatus.Running) {
                    appStatus = ApplicationStatus.SuccessfulFinish;
                } else if (appStatus == ApplicationStatus.New) {
                    // the processor is shutdown before started
                    appStatus = ApplicationStatus.UnsuccessfulFinish;
                }
            }
            if(appStatus == ApplicationStatus.UnsuccessfulFinish){
                System.exit(1);
            }
            shutdownLatch.countDown();
        }
    }
    @Override
    public void runTask() {
        JobConfig jobConfig = new JobConfig(jobModel.getConfig());

        // validation
        String taskName = new TaskConfig(config).getTaskClass().getOrElse(null);
        if (taskName == null) {
            throw new SamzaException("Neither APP nor task.class are defined defined");
        }
        log.info("LocalApplicationRunner will run " + taskName);
        LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener listener = new LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener();

        StreamProcessor processor = createStreamProcessor(jobConfig, null, listener);
        listener.setProcessor(processor);
        processor.start();
    }

    @Override
    public void run(StreamApplication app) {
        try {
            // 1. initialize and plan
            /*ExecutionPlan plan = getExecutionPlan(app);

            //String executionPlanJson = plan.getPlanAsJson();
            writePlanJsonFile(executionPlanJson);

            // 2. create the necessary streams
            // TODO: System generated intermediate streams should have robust naming scheme. See SAMZA-1391
            String planId = String.valueOf(executionPlanJson.hashCode());
            createStreams(planId, plan.getIntermediateStreams());

            // 3. create the StreamProcessors
            if (plan.getJobConfigs().isEmpty()) {
                throw new SamzaException("No jobs to run.");
            }
            plan.getJobConfigs().forEach(jobConfig -> {
                log.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
                LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener listener = new LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener();
                StreamProcessor processor = createStreamProcessor(jobConfig, app, listener);
                listener.setProcessor(processor);
                processors.add(processor);
            });*/
            JobConfig jobConfig = new JobConfig(jobModel.getConfig());
            log.info("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
            LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener listener = new LocalStreamProcessorRunner.FollowerStreamProcessorLifeCycleListener();
            StreamProcessor processor = createStreamProcessor(jobConfig, app, listener);
            listener.setProcessor(processor);
            processors.add(processor);
            numProcessorsToStart.set(processors.size());
            // 4. start the StreamProcessors
            processors.forEach(StreamProcessor::start);
        } catch (Exception e) {
            throw new SamzaException("Failed to start application", e);
        }
    }

    @Override
    public void kill(StreamApplication streamApp) {
        // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
        throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationStatus status(StreamApplication streamApp) {
        // Ultimately this class probably won't end up extending ApplicationRunner, so this will be deleted
        throw new UnsupportedOperationException();
    }
    public void waitForFinish() {
        try {
            shutdownLatch.await();
        } catch (Exception e) {
            log.error("Wait is interrupted by exception", e);
            throw new SamzaException(e);
        }
    }
    public static void main(String[] args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(
                new SamzaContainerExceptionHandler(() -> {
                    log.info("Exiting process now.");
                    System.exit(1);
                }));
        String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID());
        log.info(String.format("Got container ID: %s", containerId));
        String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
        log.info(String.format("Got coordinator URL: %s", coordinatorUrl));
        int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
        JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
        Config config = jobModel.getConfig();
        JobConfig jobConfig = new JobConfig(config);
        if (jobConfig.getName().isEmpty()) {
            throw new SamzaException("can not find the job name");
        }
        String jobName = jobConfig.getName().get();
        String jobId = jobConfig.getJobId().getOrElse(ScalaToJavaUtils.defaultValue("1"));
        MDC.put("containerName", "samza-container-" + containerId);
        MDC.put("jobName", jobName);
        MDC.put("jobId", jobId);

        StreamApplication streamApp = TaskFactoryUtil.createStreamApplication(config);
        LocalStreamProcessorRunner runner = new LocalStreamProcessorRunner(jobModel, containerId);
        runner.run(streamApp);
        runner.waitForFinish();
    }

    StreamProcessor createStreamProcessor(
            Config config,
            StreamApplication app,
            StreamProcessorLifecycleListener listener) {
        Object taskFactory = TaskFactoryUtil.createTaskFactory(config, app, new LocalApplicationRunner(config));
        if (taskFactory instanceof StreamTaskFactory) {
            return new StreamProcessor(
                    config, new HashMap<>(), (StreamTaskFactory) taskFactory, listener);
        } else if (taskFactory instanceof AsyncStreamTaskFactory) {
            return new StreamProcessor(
                    config, new HashMap<>(), (AsyncStreamTaskFactory) taskFactory, listener);
        } else {
            throw new SamzaException(String.format("%s is not a valid task factory",
                    taskFactory.getClass().getCanonicalName()));
        }
    }
}
