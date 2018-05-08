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

package org.apache.samza.task;

import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * A StreamTask is the basic class on which Samza jobs are implemented.  Developers writing Samza jobs begin by
 * implementing this class, which processes messages from the job's input streams and writes messages out to
 * streams via the provided {@link org.apache.samza.task.MessageCollector}.  A StreamTask may be augmented by
 * implementing other interfaces, such as {@link org.apache.samza.task.InitableTask}, {@link org.apache.samza.task.WindowableTask},
 * or {@link org.apache.samza.task.ClosableTask}.
 * <p>
 * The methods of StreamTasks and associated other tasks are guaranteed to be called in a single-threaded fashion;
 * no extra synchronization is necessary on the part of the class implementer.  References to instances of
 * {@link org.apache.samza.system.IncomingMessageEnvelope}s,{@link org.apache.samza.task.MessageCollector}s, and
 * {@link org.apache.samza.task.TaskCoordinator} should not be held onto between calls; there is no guarantee that
 * these will not be invalidated or otherwise used by the framework.
 *
 */
public interface StreamTask {
  /**
   * Called once for each message that this StreamTask receives.
   * @param envelope Contains the received deserialized message and key, and also information regarding the stream and
   * partition of which the message was received from.
   * @param collector Contains the means of sending message envelopes to the output stream. The collector must only
   * be used during the current call to the process method; you should not reuse the collector between invocations
   * of this method.
   * @param coordinator Manages execution of tasks.
   * @throws Exception Any exception types encountered during the execution of the processing task.
   */
  void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception;
}
