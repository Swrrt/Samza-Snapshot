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

package org.apache.samza.task

import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.container.TaskName
import org.junit.Assert._
import org.junit.Test

class TestReadableCoordinator {
  val taskName = new TaskName("P0")

  @Test
  def testCommitTask {
    val coord = new ReadableCoordinator(taskName)
    assertFalse(coord.requestedCommitTask)
    assertFalse(coord.requestedCommitAll)
    coord.commit(RequestScope.CURRENT_TASK)
    assertTrue(coord.requestedCommitTask)
    assertFalse(coord.requestedCommitAll)
  }

  @Test
  def testCommitAll {
    val coord = new ReadableCoordinator(taskName)
    assertFalse(coord.requestedCommitTask)
    assertFalse(coord.requestedCommitAll)
    coord.commit(RequestScope.ALL_TASKS_IN_CONTAINER)
    assertFalse(coord.requestedCommitTask)
    assertTrue(coord.requestedCommitAll)
  }

  @Test
  def testShutdownNow {
    val coord = new ReadableCoordinator(taskName)
    assertFalse(coord.requestedShutdownOnConsensus)
    assertFalse(coord.requestedShutdownNow)
    coord.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER)
    assertFalse(coord.requestedShutdownOnConsensus)
    assertTrue(coord.requestedShutdownNow)
  }

  @Test
  def testShutdownRequest {
    val coord = new ReadableCoordinator(taskName)
    assertFalse(coord.requestedShutdownOnConsensus)
    assertFalse(coord.requestedShutdownNow)
    coord.shutdown(RequestScope.CURRENT_TASK)
    assertTrue(coord.requestedShutdownOnConsensus)
    assertFalse(coord.requestedShutdownNow)
  }
}
