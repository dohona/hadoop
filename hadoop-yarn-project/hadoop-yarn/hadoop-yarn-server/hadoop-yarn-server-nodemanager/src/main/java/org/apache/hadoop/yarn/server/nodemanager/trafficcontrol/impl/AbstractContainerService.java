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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.ContainerService;

/**
 * 
 * Abstract class for providing container service through plugins. These plugins
 * must extend this class.
 *
 */
public abstract class AbstractContainerService
    implements ContainerService, AbstractService {

  private static final Log LOG =
      LogFactory.getLog(AbstractContainerService.class);

  public static final Pattern CONTAINER_NAME_FORMAT =
      Pattern.compile("^[a-zA-Z][\\w-]+[a-zA-Z0-9]$");

  private ContainerService callBack;

  private Map<String, Float> containerMap = new HashMap<String, Float>();

  public AbstractContainerService(Configuration conf) {

  }

  private boolean validateContainerId(String containerId) {
    if (containerId == null) {
      return false;
    }

    if (containerId.length() > 100) {
      return false;
    }

    return CONTAINER_NAME_FORMAT.matcher(containerId).matches();
  }

  /**
   * It should be called from the connection handler only.
   * 
   * @param callBack
   */
  final void setCallBack(ContainerService callBack) {
    this.callBack = callBack;
  }

  final ContainerService getCallBack() {
    return callBack;
  }

  protected abstract float normalize(float rate);

  @Override
  public final boolean addMonitoringContainer(String containerId,
      float rateInMbps) {
    if (!validateContainerId(containerId)) {
      LOG.warn(String.format("containerId is invalid: %s", containerId));
      return false;
    }

    float rate = normalize(rateInMbps);
    if (rate <= 0) {
      LOG.debug(
          String.format("The limit rate is 0! containerId: %s", containerId));
      return false;
    }

    boolean added = false;
    synchronized (containerMap) {
      if (callBack != null) {
        if (!containerMap.containsKey(containerId)) {
          added = callBack.addMonitoringContainer(containerId, rate);
          if (added) {
            containerMap.put(containerId, rate);
          }
        } else {
          added = callBack.updateContainerRate(containerId, rate);
        }
      }
    }

    return added;
  }

  @Override
  public final boolean registerPid(String containerId, int pid) {
    if (!validateContainerId(containerId)) {
      LOG.warn("containerId is invalid: "
          + containerId);
      return false;
    }

    if (pid <= 1) {
      LOG.warn("PID is invalid: "
          + pid + " for " + containerId);
      return false;
    }

    synchronized (containerMap) {
      if (callBack != null
          && containerMap.containsKey(containerId)) {
        return callBack.registerPid(containerId, pid);
      }
    }

    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.ContainerService#
   * updateContainerRate(java.lang.String, float)
   */
  @Override
  public boolean updateContainerRate(String containerId, float rateInMbps) {
    if (!validateContainerId(containerId)) {
      LOG.warn(String.format("containerId is invalid: %s", containerId));
      return false;
    }

    float rate = normalize(rateInMbps);
    if (rate <= 0) {
      LOG.debug(
          String.format("The limit rate is 0! containerId: %s", containerId));
      return false;
    }

    boolean updated = false;
    synchronized (containerMap) {
      if (callBack != null
          && containerMap.containsKey(containerId)) {
        updated = callBack.updateContainerRate(containerId, rate);
      }
    }

    return updated;
  }

  @Override
  public final void stopMonitoringContainer(String containerId) {
    if (!validateContainerId(containerId)) {
      LOG.warn("containerId is invalid: "
          + containerId);
      return;
    }
    synchronized (containerMap) {
      if (callBack != null
          && containerMap.containsKey(containerId)) {
        callBack.stopMonitoringContainer(containerId);
      }
    }
  }

  /**
   * It should be called from the connection handler only.
   * 
   * @param containerId
   */
  final void deleteCachedData(String containerId) {
    synchronized (containerMap) {
      containerMap.remove(containerId);
    }
  }

  /**
   * Return the unmodifiable view of the current containerMap.
   **/
  public Map<String, Float> getUnModifableMapOfContainers() {
    return Collections.unmodifiableMap(containerMap);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
