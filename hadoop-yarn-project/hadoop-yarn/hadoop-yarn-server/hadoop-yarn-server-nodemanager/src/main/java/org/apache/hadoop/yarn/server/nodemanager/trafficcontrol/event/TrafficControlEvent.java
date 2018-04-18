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

/**
 * TrafficControlEvent contains information of how LINUX traffic control
 * subsystem should be set.
 * <p>
 * TrafficControlEvent can be collected automatically from system information or
 * defined by enforcement handling entity.
 * <p>
 * TrafficControlEvent will be handled by TrafficControlDevice to set LTC on
 * concerned host
 */
public abstract class TrafficControlEvent {

  private String containerId;
  private TCEventType eventType;
  private boolean isLoopback;

  public TrafficControlEvent(String containerId, TCEventType eventType,
      boolean isLoopback) {
    this.containerId = containerId;
    this.eventType = eventType;
    this.isLoopback = isLoopback;
  }

  /**
   * @return the containerId
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * @return the eventType
   */
  public TCEventType getEventType() {
    return eventType;
  }

  /**
   * Only important for setting TC.
   * 
   * @return the isLoopback
   */
  public boolean isLoopback() {
    return isLoopback;
  }

}
