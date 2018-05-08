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

import org.apache.samza.system.OutgoingMessageEnvelope;

/**
 * Used as an interface for the means of sending message envelopes to an output stream.
 *
 * <p>A MessageCollector is provided on every call to {@link StreamTask#process} and
 * {@link WindowableTask#window}. You must use those MessageCollector objects only within
 * those method calls, and not hold on to a reference for use at any other time.
 */
public interface MessageCollector {
  /**
   * Sends message envelope out onto specified stream.
   * @param envelope Self contained envelope containing message, key and specified stream to be sent to.
   */
  void send(OutgoingMessageEnvelope envelope);
}
