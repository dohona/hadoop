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

/**
 * Interface for managing monitoring containers.
 *
 */
public interface ContainerService {

  /**
   * Add new container to monitor. The rate must be specified and cannot be
   * zero.
   * 
   * @param containerId
   *          the unique id of the container
   * @param rateInMbps
   *          limit rate in mbps
   * @return true if the container is added to monitor, otherwise false.
   */    
  boolean addMonitoringContainer(String containerId, float rateInMbps);

  /**
   * Update the rate of the registered container.
   * 
   * @param containerId
   *          the unique id of the container
   * @param rateInMbps
   *          limit rate in mbps
   * @return true if we can update the rate of the container.
   */
  boolean updateContainerRate(String containerId, float rateInMbps);

  /**
   * Register the id of the first process spawned by the container.
   * 
   * @param containerId
   * @param pid
   */
  boolean registerPid(String containerId, int pid);

  /**
   * Stop monitoring the container with given id.
   * 
   * @param containerId
   */
  void stopMonitoringContainer(String containerId);

}
