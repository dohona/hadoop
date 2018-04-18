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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataSubmitter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.ContainerService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.CommonProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Collect HDFS connections of containers and submit them to the back-end
 * storage.
 *
 */
public class ProcBasedConnectionHandler extends AbstractConnectionHandler
    implements AbstractService {
  static final Log LOG = LogFactory.getLog(ProcBasedConnectionHandler.class);

  static final Long WAIT_FOR_FISRT_PID = 10
      * 60 * 1000L;

  private HashMap<String, NMContainerConnections> containersMap;
  private List<NMContainerConnections> finishedContainers;
  private Lock lock = new ReentrantLock();
  private FactoryHelper helper;

  private boolean isEnabled = true;

  private int monitoringPort;
  private ConnectionMonitorThread monitorThread;

  private volatile boolean done = false;

  private List<AbstractContainerService> containerServiceList;
  private ContainerRegister containerRegister;

  private Map<String, Long> registerTime = new ConcurrentHashMap<>();

  /* Use for stored unsuccessful updated connections */

  private BiMap<String, String> containerPidMap = HashBiMap.create();

  private AbstractTCDataSubmitter submitter;
  private ProcBasedConnectionMonitor connectionMonitor;

  private Configuration conf;

  public ProcBasedConnectionHandler(Configuration conf) throws IOException {
    this(conf, FactoryHelper.getInstance());
  }

  @VisibleForTesting
  public ProcBasedConnectionHandler(Configuration conf, FactoryHelper helper)
      throws IOException {
    this.helper = helper;
    this.conf = conf;
    if (!this.helper.isTCApplicable()) {
      isEnabled = false;
      return;
    }

    submitter = this.helper.getTCDataSubmitter(conf);
    if (submitter == null) {
      LOG.warn("Submitter is null. Disable HDFS bandwidth enforcement!");
      isEnabled = false;
      return;
    }

    this.monitoringPort = conf.getInt(YarnConfiguration.NM_HDFS_PORT,
        YarnConfiguration.DEFAULT_NM_HDFS_PORT);

    containersMap = new HashMap<String, NMContainerConnections>();
    finishedContainers = new ArrayList<NMContainerConnections>();
    containerServiceList = new ArrayList<AbstractContainerService>();

    monitorThread = new ConnectionMonitorThread(conf);
    containerRegister = new ContainerRegister();

    registerContainerServices(conf);
  }

  /**
   * @return the lock
   */
  public Lock getLock() {
    return lock;
  }

  private void registerContainerServices(Configuration conf) {
    String containerServicesConf =
        conf.get(YarnConfiguration.NM_HDFS_BE_CONTAINER_PLUGINS, "");

    String[] filters = containerServicesConf.replaceAll("\\s", "").split(",");
    for (String filterName : filters) {
      if (Strings.isNullOrEmpty(filterName)) {
        continue;
      }
      LOG.info("Registering container detector plugin: "
          + filterName);
      try {
        Class<?> clazz = conf.getClassByName(filterName);
        if (clazz != null
            && AbstractContainerService.class.isAssignableFrom(clazz)) {
          Class<? extends AbstractContainerService> filterClazz =
              clazz.asSubclass(AbstractContainerService.class);
          Constructor<? extends AbstractContainerService> constructor =
              filterClazz.getConstructor(Configuration.class);
          registerContainerService(constructor.newInstance(conf));
        }
      } catch (Exception e) {
        LOG.error("Cannot initialize container detector service: "
            + filterName, e);
      }
    }

  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void
      registerContainerService(AbstractContainerService containerService) {
    if (isEnabled) {
      containerService.setCallBack(containerRegister);
      containerServiceList.add(containerService);
    } else {
      LOG.warn("Cannot register "
          + containerService + " as HDFS bandwidth enforcement is disable");
    }
  }

  /**
   * @return the monitoringPort
   */
  public int getMonitoringPort() {
    return monitoringPort;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter#start()
   */
  @Override
  public void start() {
    if (isEnabled) {
      LOG.info("Starting HDFS ConnectionHandler");
      submitter.start();
      monitorThread.start();
      for (AbstractContainerService containerService : containerServiceList) {
        try {
          containerService.start();
        } catch (Exception e) {
          LOG.error("Error occurred when starting ContainerService"
              + containerService, e);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter#stop()
   */
  @Override
  public void stop() {
    if (!isEnabled) {
      return;
    }

    LOG.info("HDFS ConnectionHandler stopping...");
    done = true;

    for (AbstractContainerService containerService : containerServiceList) {
      try {
        containerService.stop();
      } catch (Exception e) {
        LOG.error("Error occurred when stopping ContainerService"
            + containerService, e);
      }
    }

    try {
      submitter.stop();
    } catch (Exception e) {
      LOG.warn("Errors occured when stopping submitter: "
          + e.getMessage(), e);
    }

    LOG.info("HDFS ConnectionHandler stopped");
  }

  public void initialize(final String localNodeId) {
    if (!isEnabled) {
      return;
    }

    if (containerServiceList.isEmpty()) {
      LOG.fatal("At least one valid containerService "
          + " must be specify in property: "
          + YarnConfiguration.NM_HDFS_BE_CONTAINER_PLUGINS);
      isEnabled = false;
      return;
    }

    String nodeId = localNodeId;
    if (nodeId == null
        || nodeId.isEmpty()) {
      try {
        InetAddress iAddress = InetAddress.getLocalHost();
        // To get the Canonical host name
        nodeId = iAddress.getCanonicalHostName();
        LOG.info("Change hostname to: "
            + nodeId);
      } catch (UnknownHostException e) {
        LOG.warn("Inactive HDFS ConnectionHandler component: "
            + e.getMessage(), e);
        isEnabled = false;
        return;
      }
    }

    LOG.info("Initializing HDFS ConnectionHandler for "
        + nodeId);

    if (connectionMonitor == null) {
      setConnectionMonitor(new ProcBasedConnectionMonitor(conf));
    }

    for (AbstractContainerService containerService : containerServiceList) {
      try {
        containerService.initialize(nodeId);
      } catch (Exception e) {
        LOG.error("Error occurred when initializing ContainerService"
            + containerService, e);
      }
    }

    submitter.initialize(nodeId);
  }

  private boolean addTcClassView(String containerId, float rateInMbps) {
    if (rateInMbps > 0) {
      return addTcClassView(new NMContainerConnections(containerId, rateInMbps,
          this.monitoringPort));
    } else {
      LOG.info("No limit set for HDFS bandwidth of container "
          + containerId);
    }

    return false;
  }

  private boolean updateTcClassView(String containerId, float rateInMbps) {
    try {
      lock.lock();

      if (rateInMbps <= 0) {
        LOG.warn("Invalid zero rate of HDFS bandwidth is given for container "
            + containerId + ". Skip it!");
        return false;
      }

      if (!containersMap.containsKey(containerId)) {
        return addTcClassView(containerId, rateInMbps);
      }

      NMContainerConnections container = containersMap.get(containerId);
      boolean updated = false;
      if (container != null) {
        updated = container.setRate(rateInMbps);
      }
      if (updated) {
        LOG.info("Change rate of container: "
            + containerId + ", new rate is " + container.getRate());
      } else {
        LOG.warn("Same rate of HDFS bandwidth is given for container "
            + containerId + ". Skip it!");
      }
      return updated;
    } finally {
      lock.unlock();
    }
  }

  public boolean addTcClassView(NMContainerConnections view) {
    try {
      lock.lock();
      final String containerId = view.getContainerId();
      if (!containersMap.containsKey(containerId)) {
        LOG.info("Add new container to track: "
            + containerId + ", rate is " + view.getRate());
        containersMap.put(containerId, view);
        registerTime.put(containerId, System.currentTimeMillis());

        return true;
      }

      return false;

    } finally {
      lock.unlock();
    }
  }

  public boolean registerContainer(String containerId, String pid) {
    try {
      lock.lock();
      if (!containerPidMap.containsKey(containerId)) {
        Path pidFile =
            Paths.get(FactoryHelper.getInstance().getProcFsRoot(), pid);
        if (Files.exists(pidFile)) {
          LOG.info(String.format("Register container: %s, pid: %s", containerId,
              pid));
          containerPidMap.put(containerId, pid);
          NMContainerConnections con = containersMap.get(containerId);
          if (con != null) {
            con.initialized();
            CommonProcessTree pTree = connectionMonitor.getProcfsTree(pid);
            if (pTree != null) {
              con.updateProcessTree(pTree);
            }
          }
          return true;
        } else {
          LOG.warn(String.format("Invalid pid %s is reported for container: %s."
              + " Stop monitoring the container!", pid, containerId));
          removeTcClassView(containerId);
        }
      }
    } finally {
      lock.unlock();
    }
    return false;
  }

  @Override
  public synchronized Map<String, List<DNContainerConnections>> collect() {
    return connectionMonitor.collect();
  }

  @Override
  public boolean submit(String host, List<DNContainerConnections> data) {
    try {
      lock.lock();
      if (done) {
        return false;
      }
      boolean ok = submitter.submit(host, data);
      if (ok) {
        connectionMonitor.getConnectionsData().remove(host);
      }

      return true;
    } finally {
      lock.unlock();
    }
  }

  public void removeTcClassView(String containerId) {
    try {
      lock.lock();
      LOG.info("Stop monitoring container: "
          + containerId);
      NMContainerConnections tcClass = containersMap.get(containerId);
      if (tcClass != null) {
        tcClass.stopTrackContainer();
        finishedContainers.add(tcClass);
        containersMap.remove(containerId);
      }
      containerPidMap.remove(containerId);
      registerTime.remove(containerId);

      for (AbstractContainerService service : containerServiceList) {
        service.deleteCachedData(containerId);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param monitor
   *          the connectionMonitor to set
   */
  public void setConnectionMonitor(ProcBasedConnectionMonitor monitor) {
    if (this.connectionMonitor == null) {
      this.connectionMonitor = monitor;
      this.connectionMonitor.setConnectionHandler(this);
    }
  }

  /**
   * @return the containersMap
   */
  public Map<String, NMContainerConnections> getContainersMap() {
    return containersMap;
  }

  /**
   * @return the finishedContainers
   */
  public List<NMContainerConnections> getFinishedContainers() {
    return finishedContainers;
  }

  public void
      buildContainerTrees(final Map<String, CommonProcessTree> processTree) {
    Iterator<Entry<String, NMContainerConnections>> entries =
        containersMap.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<String, NMContainerConnections> entry = entries.next();
      String containerId = entry.getKey();
      NMContainerConnections nmContainer = entry.getValue();
      String pid = containerPidMap.get(containerId);
      if (pid != null) {
        nmContainer.updateProcessTree(processTree.get(pid));
      } else {
        nmContainer.setProcessTree(null);
      }
    }
  }

  public void removeInvalidPid(String invalidPid) {
    final String containerId = containerPidMap.inverse().get(invalidPid);
    if (!Strings.isNullOrEmpty(containerId)) {
      removeTcClassView(containerId);
    }
  }

  class ConnectionMonitorThread extends Thread {
    private long monitoringInterval;
    private long checkOrphanContainersInterval = 5
        * 60 * 1000L;
    private long checkOrphanContainerTime = checkOrphanContainersInterval;

    public ConnectionMonitorThread(Configuration conf) {
      super("Collector thread for collecting TC data");
      this.monitoringInterval =
          conf.getLong(YarnConfiguration.NM_HDFS_BE_CHECK_TC_INTERVAL_MS,
              YarnConfiguration.DEFAULT_NM_HDFS_BE_CHECK_TC_INTERVAL_MS);
      if (checkOrphanContainersInterval < 2
          * monitoringInterval) {
        checkOrphanContainersInterval = 2
            * monitoringInterval;
      }

      checkOrphanContainerTime = checkOrphanContainersInterval;
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(monitoringInterval);
          if (done
              || Thread.interrupted()) {
            break;
          }

          if (containersMap.isEmpty()
              && finishedContainers.isEmpty()) {
            continue;
          }

          process();
          checkOrphanContainerTime -= monitoringInterval;

          if (checkOrphanContainerTime <= 0) {
            checkOrphanContainerTime = checkOrphanContainersInterval;
            checkInvalidContainers();
          }

        } catch (InterruptedException e) {
          LOG.warn(ConnectionMonitorThread.class.getName()
              + " is interrupted.");
        } catch (Exception e) {
          LOG.warn("Something wrong when processing live connections", e);
        }
      }
    } // end run

    private void checkInvalidContainers() {
      LOG.debug("Check orphan containers (without PID after 10 minutes)");
      long currentTime = System.currentTimeMillis();
      Set<String> invalidContainers = new HashSet<String>();
      for (Entry<String, NMContainerConnections> entry : containersMap
          .entrySet()) {
        String cID = entry.getKey();
        NMContainerConnections con = entry.getValue();
        if (!con.isInitialized()
            && registerTime.containsKey(cID)) {
          if (currentTime
              - registerTime.get(cID) >= WAIT_FOR_FISRT_PID) {
            LOG.warn(cID
                + " doesn't have pid for at least 10 minutes. Remove it");
            invalidContainers.add(cID);
          }
        }
      }
      for (String cID : invalidContainers) {
        removeTcClassView(cID);
      }
    }
  } // end ConnectionMonitorSubmitThread

  private class ContainerRegister implements ContainerService {

    @Override
    public boolean addMonitoringContainer(String containerId, float rate) {
      if (isEnabled) {
        return addTcClassView(containerId, rate);
      }

      return false;
    }

    @Override
    public boolean registerPid(String containerId, int pid) {
      if (isEnabled) {
        return registerContainer(containerId, Integer.toString(pid));
      }

      return false;
    }

    @Override
    public void stopMonitoringContainer(String containerId) {
      if (isEnabled) {
        removeTcClassView(containerId);
      }
    }

    @Override
    public boolean updateContainerRate(String containerId, float rateInMbps) {
      if (isEnabled) {
        return updateTcClassView(containerId, rateInMbps);
      }

      return false;
    }
  }
}
