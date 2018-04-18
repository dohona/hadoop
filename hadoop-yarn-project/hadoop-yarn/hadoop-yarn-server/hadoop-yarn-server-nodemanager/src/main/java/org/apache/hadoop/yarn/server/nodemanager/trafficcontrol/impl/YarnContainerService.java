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

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractYarnContainerReportService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

/**
 * This plug-in provides the container service for
 * {@link ProcBasedConnectionHandler}. It is called from
 * {@code ContainersMonitorImpl}.
 *
 */
public class YarnContainerService extends AbstractContainerService {

  private static final Log LOG = LogFactory.getLog(YarnContainerService.class);

  private float minRate = 0;
  private float maxRate = 1_000_000; // 1000Gb

  private boolean isClientMode = false;

  public YarnContainerService(Configuration conf, boolean isClientMode) {
    super(conf);
    float configuredMinRate =
        conf.getFloat(YarnConfiguration.NM_HDFS_BE_MIN_RATE, 0);
    minRate = Math.min(configuredMinRate, 1_000);
    maxRate = conf.getFloat(YarnConfiguration.NM_HDFS_BE_MAX_RATE, 1_000_000);

    if (maxRate
        - minRate < 1) {
      maxRate = 1_000_000;
    }

    LOG.info(String.format(
        "Min HDFS rate allocation for YARN containers: %.2fmbps", minRate));
    LOG.info(String.format(
        "Max HDFS rate allocation for YARN containers: %.2fmbps", maxRate));

    this.isClientMode = isClientMode;
  }

  public static YarnContainerService
      loadYarnContainerService(Configuration conf) {
    return loadYarnContainerService(conf, FactoryHelper.getInstance());
  }

  @VisibleForTesting
  public static YarnContainerService loadYarnContainerService(
      Configuration conf, FactoryHelper helper) {
    boolean isEnabled =
        conf.getBoolean(YarnConfiguration.NM_HDFS_BE_ENABLE,
            YarnConfiguration.DEFAULT_NM_HDFS_BE_ENABLE)
            && helper.isTCApplicable();

    LOG.info("HDFS bandwidth enforcement: "
        + isEnabled);

    if (!isEnabled) {
      return null;
    }

    if (!conf.getBoolean(YarnConfiguration.NM_HDFS_BE_CLIENT_MODE, false)) {
      return new YarnContainerService(conf, false);
    }

    String reporterClazzName =
        conf.get(YarnConfiguration.NM_HDFS_BE_REPORT_SERVICE_CLASS);
    if (!Strings.isNullOrEmpty(reporterClazzName)) {
      LOG.info("Register container report plugin: "
          + reporterClazzName);

      try {
        Class<? extends AbstractYarnContainerReportService> reportClazz =
            conf.getClass(YarnConfiguration.NM_HDFS_BE_REPORT_SERVICE_CLASS,
                null, AbstractYarnContainerReportService.class);

        if (reportClazz == null) {
          LOG.error("Cannot load the report plugin defined in property: "
              + YarnConfiguration.NM_HDFS_BE_REPORT_SERVICE_CLASS);
          return null;
        }

        Constructor<? extends AbstractYarnContainerReportService> constructor =
            reportClazz.getConstructor(Configuration.class);
        YarnContainerService containerService =
            new YarnContainerService(conf, true);
        containerService.setCallBack(constructor.newInstance(conf));
        return containerService;
      } catch (Exception e) {
        LOG.error("Cannot set callback service: "
            + e.getMessage(), e);
      }
    }

    return null;
  }

  /**
   * @return the isClientMode
   */
  public boolean isClientMode() {
    return isClientMode;
  }

  public void stopMonitoringContainer(ContainerId con) {
    stopMonitoringContainer(con.toString());
  }

  public void addMonitoringContainer(ContainerId con, float rateInMbps) {
    addMonitoringContainer(con.toString(), rateInMbps);
  }

  public void registerPid(ContainerId containerId, String pId) {
    registerPid(containerId.toString(), Integer.parseInt(pId));
  }

  @Override
  protected float normalize(float rate) {
    float newRate = Math.min(Math.max(rate, minRate), maxRate);
    if (newRate != rate) {
      LOG.info("Normalized HDFS read bandwidth limit: "
          + newRate + " mbps");
    }
    return newRate;
  }

  @Override
  public void initialize(String localNodeId) {
    if (isClientMode) {
      ((AbstractYarnContainerReportService) getCallBack())
          .initialize(localNodeId);
    }
  }

  @Override
  public void start() {
    if (isClientMode) {
      ((AbstractYarnContainerReportService) getCallBack()).start();
    }
  }

  @Override
  public void stop() {

    try {
      Set<String> containerIds =
          new HashSet<String>(getUnModifableMapOfContainers().keySet());
      for (String containerId : containerIds) {
        stopMonitoringContainer(containerId);
      }
    } catch (Exception e) {
      ;
    }

    if (isClientMode) {
      ((AbstractYarnContainerReportService) getCallBack()).stop();
    }
  }

  /**
   * @return the minRate
   */
  public float getMinRate() {
    return minRate;
  }

  /**
   * @return the maxRate
   */
  public float getMaxRate() {
    return maxRate;
  }
}
