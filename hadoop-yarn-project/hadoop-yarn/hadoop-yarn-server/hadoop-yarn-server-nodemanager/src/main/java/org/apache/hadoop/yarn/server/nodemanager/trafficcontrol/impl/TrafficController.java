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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractAsyncTCDataCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTrafficController;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCClassEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCEventType;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCFilterEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TrafficControlEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor.TrafficControlDeviceExecutor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * This class collects TC settings from HDFS storage and apply them to the given
 * device.
 *
 */
public class TrafficController extends AbstractTrafficController
    implements AbstractService {

  static final Log LOG = LogFactory.getLog(TrafficController.class);

  private List<TrafficControlDeviceExecutor> deviceExecutors;
  private AbstractTCDataCollector dataCollector;
  private CollectorThread collectorThread = null;
  private ConnectionCollector connectionCollector;

  private String localNodeId;
  private int syncLoops = 5;
  private int monitoringPort = 50010;

  private volatile boolean done = false;
  private boolean isEnabled = true;

  private boolean asyncTCDataCollectorIsUsed = false;
  private FactoryHelper helper = null;

  private Table<String, String, DNContainerConnections> connectionDb;
  private Map<String, Connection> connectionCache =
      new HashMap<String, Connection>();

  public TrafficController(Configuration conf) {
    this(conf, false, FactoryHelper.getInstance(), 5);
  }

  public TrafficController(Configuration conf, boolean runInNodeManager) {
    this(conf, runInNodeManager, FactoryHelper.getInstance(), 5);
  }

  @VisibleForTesting
  public TrafficController(Configuration conf, boolean runInNodeManager,
      FactoryHelper helper, int syncLoops) {
    if (runInNodeManager
        && !conf.getBoolean(YarnConfiguration.NM_HDFS_BE_ENABLE,
            YarnConfiguration.DEFAULT_NM_HDFS_BE_ENABLE)) {
      LOG.info("Traffic Control executor for Hdfs"
          + " bandwidth enforcement is not enabled");
      isEnabled = false;
      return;
    }

    this.helper = helper;
    isEnabled = this.helper.isTCApplicable();
    if (!isEnabled) {
      return;
    }

    this.syncLoops = syncLoops;

    if (runInNodeManager) {
      if (conf.getBoolean(YarnConfiguration.NM_HDFS_BE_CLIENT_MODE, false)) {
        LOG.warn("HDFS bandwidth enforcement is run in client mode."
            + " Do not start Traffic Control executor!");
        isEnabled = false;
        return;
      }

      LOG.info("Traffic Control executor for Hdfs "
          + "bandwidth enforcement enabled:" + isEnabled);

      if (conf.getBoolean(YarnConfiguration.NM_HDFS_BE_ENABLE_SUBMITTER_ONLY,
          false)) {
        LOG.info("Only Submitter for Hdfs bandwidth enforcement is enabled!");
        isEnabled = false;
        return;
      }
    }

    dataCollector = helper.getTCDataCollector(conf);
    if (dataCollector == null) {
      LOG.warn("Collector is null. Disable HDFS bandwidth enforcement!");
      isEnabled = false;
      return;
    }

    if (dataCollector instanceof AbstractAsyncTCDataCollector) {
      asyncTCDataCollectorIsUsed = true;
    }

    this.monitoringPort = conf.getInt(YarnConfiguration.NM_HDFS_PORT,
        YarnConfiguration.DEFAULT_NM_HDFS_PORT);
    this.connectionCollector =
        this.helper.getConnectionCollector(monitoringPort, true);

    this.deviceExecutors = new ArrayList<TrafficControlDeviceExecutor>();
    String deviceList = conf.get(YarnConfiguration.NM_HDFS_BE_DEVICES,
        YarnConfiguration.DEFAULT_NM_HDFS_BE_DEVICES);
    deviceList = deviceList.replaceAll("\\s", "");
    List<String> deviceNames = Arrays.asList(deviceList.split(","));
    for (String deviceName : deviceNames) {
      deviceExecutors
          .add(this.helper.getDevice(deviceName, monitoringPort, helper));
    }

    if (!asyncTCDataCollectorIsUsed) {
      collectorThread = new CollectorThread(conf);
    }

    connectionDb = HashBasedTable.create();
  }

  /**
   * @return the monitoringPort
   */
  public int getMonitoringPort() {
    return monitoringPort;
  }

  @Override
  public void start() {
    if (!isEnabled) {
      return;
    }
    LOG.info("Starting HDFS TrafficController");
    dataCollector.start();

    if (collectorThread != null) {
      collectorThread.start();
    }
  }

  @Override
  public void stop() {
    if (!isEnabled) {
      return;
    }
    LOG.info("HDFS TrafficController stopping...");
    done = true;

    try {
      dataCollector.stop();
    } catch (Exception e) {
      LOG.warn("Error occurred when stopping collector: "
          + e.getMessage(), e);
    }

    LOG.info("HDFS TrafficController stopped");
  }

  /**
   * @return the isEnabled
   */
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void initialize(final String nodeId) {
    if (!isEnabled) {
      return;
    }

    this.localNodeId = nodeId;
    if (localNodeId == null
        || localNodeId.isEmpty()) {
      try {
        InetAddress iAddress = InetAddress.getLocalHost();
        // To get the Canonical host name
        localNodeId = iAddress.getCanonicalHostName();
        LOG.info("Change hostname to: "
            + localNodeId);
      } catch (UnknownHostException e) {
        LOG.warn("Inactive HDFS TrafficController component: "
            + e.getMessage(), e);
        isEnabled = false;
        return;
      }
    }

    LOG.info("Initializing HDFS TrafficController for "
        + localNodeId);

    Iterator<TrafficControlDeviceExecutor> iterator =
        deviceExecutors.listIterator();

    while (iterator.hasNext()) {
      TrafficControlDeviceExecutor device = iterator.next();
      try {
        device.initDevice();
      } catch (IOException e) {
        LOG.error("Cannot init the device: "
            + device.getName() + ". Do not use this device!", e);
        iterator.remove();
      }
    }

    if (deviceExecutors.isEmpty()) {
      LOG.error("Cannot init any devices. Exit!");
      isEnabled = false;
      return;
    }

    if (asyncTCDataCollectorIsUsed) {
      ((AbstractAsyncTCDataCollector) dataCollector)
          .registerCallback((AbstractTrafficController) this);
    }

    dataCollector.initialize(localNodeId);
  }

  @Override
  public void execute(List<TrafficControlEvent> events) {
    if (!events.isEmpty()) {
      for (TrafficControlDeviceExecutor device : deviceExecutors) {
        device.update(events);
      }
    }
  }

  @Override
  public Map<String, List<DNContainerConnections>> collectData() {
    if (!done) {
      return dataCollector.collectData();
    }

    return null;
  }

  @Override
  public List<TrafficControlEvent>
      buildTCEvents(Map<String, List<DNContainerConnections>> connections) {
    List<TrafficControlEvent> eventList = new ArrayList<TrafficControlEvent>();

    if (connections.isEmpty()) {
      return eventList;
    }
    List<TCClassEvent> delClassList = new ArrayList<TCClassEvent>();
    List<TCClassEvent> addClassList = new ArrayList<TCClassEvent>();
    List<TCClassEvent> changeClassList = new ArrayList<TCClassEvent>();
    List<TCFilterEvent> delFilterList = new ArrayList<TCFilterEvent>();
    List<TCFilterEvent> addFilterList = new ArrayList<TCFilterEvent>();

    final Collection<Connection> currentNMConnections =
        collectCurrentConnections();

    Predicate<Connection> validConnection = new Predicate<Connection>() {
      @Override
      public boolean apply(Connection connection) {
        return currentNMConnections == null
            || currentNMConnections.contains(connection);
      }
    };

    synchronized (connectionDb) {

      for (Entry<String, List<DNContainerConnections>> entry : connections
          .entrySet()) {
        final String remoteNMHost = entry.getKey();

        boolean isLoopback = localNodeId.equals(remoteNMHost);

        List<DNContainerConnections> nmContainers = entry.getValue();
        Map<String, DNContainerConnections> dbContainers =
            connectionDb.row(remoteNMHost);

        if (nmContainers == null
            || nmContainers.isEmpty()) {
          // Disconnected host. Delete its containers!
          for (DNContainerConnections delContainer : dbContainers.values()) {
            deleteTCClass(remoteNMHost, delContainer, delClassList,
                delFilterList);
          }
        } else if (dbContainers.isEmpty()) { // New hosts
          for (DNContainerConnections newContainer : nmContainers) {
            final String containerId = newContainer.getContainerId();
            if (newContainer.getConnections().isEmpty()) {
              LOG.warn(containerId
                  + " hasn't got any connections. Do not add it!");
              continue;
            }

            addNewTCClass(remoteNMHost, containerId, newContainer,
                validConnection, addClassList, addFilterList);
          }
        } else { // NM hosts with updated connections
          Set<String> storedContainerIds =
              new HashSet<String>(dbContainers.keySet());
          for (DNContainerConnections nmContainer : nmContainers) {
            final String containerId = nmContainer.getContainerId();
            List<Connection> updatedConnections = nmContainer.getConnections();

            if (!storedContainerIds.contains(containerId)) { // new container
              addNewTCClass(remoteNMHost, containerId, nmContainer,
                  validConnection, addClassList, addFilterList);
            } else {
              // this is existing class. Synchronize its connections
              boolean hasConnections = false;
              storedContainerIds.remove(containerId);
              List<Connection> storedConnections =
                  dbContainers.get(containerId).getConnections();

              // Synchronize connections
              Iterator<Connection> iter = updatedConnections.iterator();
              while (iter.hasNext()) {
                Connection connection = iter.next();
                if (validConnection.apply(connection)) {
                  // new connection
                  if (!storedConnections.contains(connection)) {
                    // Create addFilter events
                    addFilterList.add(TCFilterEvent
                        .createAddFilterEvent(containerId, connection));
                  } else { // This is existed.
                    storedConnections.remove(connection);
                  }

                  hasConnections = true;
                } else {
                  iter.remove();
                }
              }

              // Create delFilter events for disconnected ones
              for (Connection connection : storedConnections) {
                delFilterList.add(TCFilterEvent
                    .createDelFilterEvent(containerId, connection));
              }

              if (!hasConnections) {
                // Create delClass event
                delClassList.add(new TCClassEvent(containerId,
                    TCEventType.DEL_CLASS, "", isLoopback));
                // Delete from DB
                connectionDb.remove(remoteNMHost, containerId);
              } else {
                String oldRate = dbContainers.get(containerId).getRate();
                if (!oldRate.equals(nmContainer.getRate())) {
                  changeClassList.add(
                      new TCClassEvent(containerId, TCEventType.CHANGE_CLASS,
                          nmContainer.getRate(), isLoopback));
                }
                // Update the DB.
                connectionDb.put(remoteNMHost, containerId, nmContainer);
              }
            }
          }

          // Deleted containers
          for (String containerId : storedContainerIds) {
            deleteTCClass(remoteNMHost, dbContainers.get(containerId),
                delClassList, delFilterList);
          }
        }
      }

      eventList.addAll(delFilterList);
      eventList.addAll(delClassList);
      eventList.addAll(changeClassList);
      eventList.addAll(addClassList);
      eventList.addAll(addFilterList);
    }
    return eventList;
  }

  /**
   * Delete TC class ralated to a given container and all their filters.
   * 
   * @param remoteNMHost
   * @param deletedContainer
   * @param delClassList
   * @param delFilterList
   */
  private void deleteTCClass(final String remoteNMHost,
      DNContainerConnections deletedContainer, List<TCClassEvent> delClassList,
      List<TCFilterEvent> delFilterList) {
    final String containerId = deletedContainer.getContainerId();

    // Create delClass event
    delClassList.add(new TCClassEvent(containerId, TCEventType.DEL_CLASS, "",
        localNodeId.equals(remoteNMHost)));

    // Create delFilter events
    for (Connection connection : deletedContainer.getConnections()) {
      delFilterList
          .add(TCFilterEvent.createDelFilterEvent(containerId, connection));
    }

    // Delete from DB
    connectionDb.remove(remoteNMHost, containerId);
  }

  /**
   * Create new TC class and its filter events for new container.
   * 
   * @param remoteNMHost
   * @param containerId
   * @param container
   * @param validConnection
   * @param addClassList
   * @param addFilterList
   */
  private void addNewTCClass(final String remoteNMHost,
      final String containerId, DNContainerConnections container,
      Predicate<Connection> validConnection, List<TCClassEvent> addClassList,
      List<TCFilterEvent> addFilterList) {
    boolean hasConnections = false;

    List<Connection> connections = container.getConnections();

    // Create addFilter events
    for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
      Connection connection = iter.next();
      if (validConnection.apply(connection)) {
        addFilterList
            .add(TCFilterEvent.createAddFilterEvent(containerId, connection));
        hasConnections = true;
      } else {
        iter.remove();
      }
    }

    // Create addClass event
    if (hasConnections) {
      addClassList.add(new TCClassEvent(containerId, TCEventType.ADD_CLASS,
          container.getRate(), localNodeId.equals(remoteNMHost)));
      // Put to the DB.
      connectionDb.put(remoteNMHost, containerId, container);
    }
  }

  /**
   * Collect all connections from port of DataNode.
   * 
   * @return list of connections from HDFS port
   */
  @VisibleForTesting
  public Collection<Connection> collectCurrentConnections() {
    if (syncLoops > 0) {
      syncLoops--;
      if (syncLoops == 0) {
        return null;
      }
      connectionCollector.collectConnections(false, connectionCache);
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        ;
      }
      connectionCollector.collectConnections(false, connectionCache);
      connectionCollector.collectConnections(true, connectionCache);

      return connectionCache.values();
    }

    return null;
  }

  class CollectorThread extends Thread {
    private Long monitoringInterval = 500L;

    public CollectorThread(Configuration conf) {
      super("Collector thread for collecting TC data");
      this.monitoringInterval =
          conf.getLong(YarnConfiguration.NM_HDFS_BE_CHECK_TC_INTERVAL_MS,
              YarnConfiguration.DEFAULT_NM_HDFS_BE_CHECK_TC_INTERVAL_MS);
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

          if (dataCollector.isReady()) {
            update();
          }
        } catch (InterruptedException e) {
          LOG.warn(CollectorThread.class.getName()
              + " is interrupted.");
        } catch (Exception e) {
          ;
        }
      }
    }
  }
}
