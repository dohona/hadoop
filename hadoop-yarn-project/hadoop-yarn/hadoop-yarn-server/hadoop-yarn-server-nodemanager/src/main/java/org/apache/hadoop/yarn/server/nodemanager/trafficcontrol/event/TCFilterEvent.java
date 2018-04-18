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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event;

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;

/**
 * Data class for an LTC filter request.
 *
 */
public class TCFilterEvent extends TrafficControlEvent {
  private int localPort;
  private String remoteHost;
  private int remotePort;

  public TCFilterEvent(String containerId, TCEventType eventType,
      Connection connection) {
    super(containerId, eventType, connection.isLoopback());
    this.localPort = connection.getSrcPort();
    this.remoteHost = connection.getDstHost();
    this.remotePort = connection.getDstPort();
  }

  /**
   * @return the localPort
   */
  public int getLocalPort() {
    return localPort;
  }

  /**
   * @return the remoteHost
   */
  public String getRemoteHost() {
    return remoteHost;
  }

  /**
   * @return the remotePort
   */
  public int getRemotePort() {
    return remotePort;
  }

  public static TCFilterEvent createDelFilterEvent(String containerId,
      Connection connection) {
    return new TCFilterEvent(containerId, TCEventType.DEL_FILTER, connection);
  }

  public static TCFilterEvent createAddFilterEvent(String containerId,
      Connection connection) {
    return new TCFilterEvent(containerId, TCEventType.ADD_FILTER, connection);
  }

}
