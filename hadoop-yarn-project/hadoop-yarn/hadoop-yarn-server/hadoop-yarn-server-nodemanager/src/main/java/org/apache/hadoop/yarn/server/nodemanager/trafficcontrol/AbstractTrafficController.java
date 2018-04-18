/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TrafficControlEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

/**
 * Retry {@link TrafficControlEvent} from persistence the execute appropriate
 * traffic control setting.
 * 
 */
public abstract class AbstractTrafficController implements
    TrafficControlExecutor, TrafficControlDataCollector, LTCEventBuilder {

  /**
   * Collect the updated hosts and their connections, then synchronize with
   * stored database and the current list of connections. Finally it will
   * generate the list of TC events to be executed by devices. It is called
   * periodically in case of sync update or through the notifyUpdate() of async
   * update.
   */
  public synchronized void update() {
    Map<String, List<DNContainerConnections>> partialConnections =
        collectData();
    if (partialConnections != null
        && !partialConnections.isEmpty()) {
      List<TrafficControlEvent> events = buildTCEvents(partialConnections);
      if (!events.isEmpty()) {
        execute(events);
      }
    }
  }
}
