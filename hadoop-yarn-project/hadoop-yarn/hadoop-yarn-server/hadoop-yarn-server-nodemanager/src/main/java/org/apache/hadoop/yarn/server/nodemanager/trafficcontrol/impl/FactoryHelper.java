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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataSubmitter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor.TCCommand;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor.TrafficControlDeviceExecutor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;

/**
 * Helper class.
 *
 */
public class FactoryHelper {

  static final Log LOG = LogFactory.getLog(FactoryHelper.class);
  private static FactoryHelper instance = null;

  private String procFsRoot = "/proc";

  private FactoryHelper() {
    return;
  }

  public static FactoryHelper getInstance() {
    if (instance == null) {
      instance = new FactoryHelper();
    }

    return instance;
  }

  /**
   * @return the procFsRoot
   */
  public String getProcFsRoot() {
    return procFsRoot;
  }

  /**
   * @param procFsRoot
   *          the procFsRoot to set
   */
  public void setProcFsRoot(String procFsRoot) {
    this.procFsRoot = procFsRoot;
  }

  public AbstractTCDataSubmitter getTCDataSubmitter(Configuration pConf) {

    Class<? extends AbstractTCDataSubmitter> submitterClazz =
        pConf.getClass(YarnConfiguration.NM_HDFS_BE_SUBMITTER_CLASS,
            HdfsTCDataSubmitter.class, AbstractTCDataSubmitter.class);

    if (submitterClazz != null) {
      try {
        Constructor<? extends AbstractTCDataSubmitter> c =
            submitterClazz.getConstructor(Configuration.class);
        AbstractTCDataSubmitter pSubmitter = c.newInstance(pConf);
        LOG.info("Loaded plugin "
            + pSubmitter.getClass().getName());
        return pSubmitter;
      } catch (Exception e) {
        LOG.error("Cannot initialize submitter: "
            + e.getMessage(), e);
      }
    }
    return null;
  }

  public TrafficControlDeviceExecutor getDevice(String deviceName,
      int monitoringPort, FactoryHelper helper) {
    return new TrafficControlDeviceExecutor(deviceName, monitoringPort, helper);
  }

  public AbstractTCDataCollector getTCDataCollector(Configuration pConf) {
    Class<? extends AbstractTCDataCollector> collectorClazz =
        pConf.getClass(YarnConfiguration.NM_HDFS_BE_COLLECTOR_CLASS,
            HdfsTCDataCollector.class, AbstractTCDataCollector.class);

    if (collectorClazz != null) {
      try {
        Constructor<? extends AbstractTCDataCollector> c =
            collectorClazz.getConstructor(Configuration.class);
        AbstractTCDataCollector pCollector = c.newInstance(pConf);
        LOG.info("Loaded plugin "
            + pCollector.getClass().getName());
        return pCollector;
      } catch (Exception e) {
        LOG.error("Cannot initialize collector: "
            + e.getMessage(), e);
      }
    }

    return null;
  }

  public ConnectionCollector getConnectionCollector(int monitoringPort,
      boolean isDataNode) {
    return new ConnectionCollector(monitoringPort, isDataNode);

  }

  public int exec(TCCommand tcCmd) {
    Process p;
    try {
      p = Runtime.getRuntime().exec(tcCmd.toString());
      p.waitFor();
      return p.exitValue();
    } catch (IOException e) {
      LOG.error("Error occurred when executing command: "
          + tcCmd, e);
    } catch (InterruptedException e) {
      ;
    }
    return -1;
  }

  public BufferedReader getBufferedReader(TCCommand tcCmd) throws IOException {
    Process p = Runtime.getRuntime().exec(tcCmd.toString());
    BufferedReader input =
        new BufferedReader(new InputStreamReader(p.getInputStream(),
            StandardCharsets.UTF_8));

    return input;
  }

  /**
   * Checks if Linux Traffic Control can be applied on this system.
   *
   * @return true if the system is Linux. False otherwise.
   */
  public boolean isTCApplicable() {
    try {
      if (!Shell.LINUX) {
        LOG.info("HDFS bandwidth enforcement currently is supported only on "
            + "Linux.");
        return false;
      }
    } catch (SecurityException se) {
      LOG.warn("Failed to get Operating System name. "
          + se.getMessage(), se);
      return false;
    }
    return true;
  }
}
