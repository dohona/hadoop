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
 * Interface for constructing LTC settings.
 *
 */
public interface LTCEventBuilder {
  /**
   * Construct TC events based on the input connections data.
   * 
   * @param connections
   *          list of connections of the updated remote hosts
   * @return list of TC events to be executed
   */
  List<TrafficControlEvent> buildTCEvents(
      Map<String, List<DNContainerConnections>> connections);
}
