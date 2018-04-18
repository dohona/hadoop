/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.ConnectionMonitor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.CommonProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * This class monitors the /proc file system and collect network data for
 * containers.
 *
 */
public class ProcBasedConnectionMonitor implements ConnectionMonitor {

  static final Log LOG = LogFactory.getLog(ProcBasedConnectionMonitor.class);

  public static final String CONNECTED_DN_FILE_NAME = "connected_dn_nodes.dat";

  static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern
      .compile("^([0-9-]+)\\s+\\(([^\\s]+)\\)\\s[^\\s]\\s([0-9-]+)\\s.+");

  static final Pattern VALID_PID = Pattern.compile("^(\\d+)$");

  private Set<String> connectedHosts = new HashSet<String>();
  private Map<String, String> pidMap = Maps.newHashMap();

  private Joiner joiner = Joiner.on(",");
  private Path backupConnectedDNsFile = Paths.get(
      System.getProperty("java.io.tmpdir"), CONNECTED_DN_FILE_NAME);

  private Map<String, List<DNContainerConnections>> connectionsData =
      new ConcurrentHashMap<String, List<DNContainerConnections>>();

  private Map<String, CommonProcessTree> procfsTree =
      new HashMap<String, CommonProcessTree>();

  private ConnectionCollector collector;
  private ProcBasedConnectionHandler connectionHandler;
  private Lock lock;
  private Configuration conf;

  public ProcBasedConnectionMonitor(Configuration conf) {
    this.conf = conf;
  }

  public void
      setConnectionHandler(ProcBasedConnectionHandler connectionHandler) {
    if (this.connectionHandler == null) {
      this.connectionHandler = connectionHandler;
      init();
    }
  }

  /**
   * @param collector
   *          the collector to set
   */
  public void setCollector(ConnectionCollector collector) {
    if (this.collector == null) {
      this.collector = collector;
    }
  }

  public void setBackupConnectedDNsFile(Path path) {
    backupConnectedDNsFile = path;
  }

  public void init() {
    lock = connectionHandler.getLock();
    if (collector == null) {
      int monitoringPort = connectionHandler.getMonitoringPort();
      collector = new ConnectionCollector(monitoringPort, false);
      boolean canExecuteSudoSS =
          conf.getBoolean(YarnConfiguration.NM_HDFS_BE_EXECUTE_SUDO_SS, false);
      if (canExecuteSudoSS) {
        collector.sudoIsAvailabe();
      }
    }

    try {
      if (Files.exists(backupConnectedDNsFile)) {
        String connectedHostIpList = readFirstLine(backupConnectedDNsFile);
        connectedHostIpList = connectedHostIpList.replaceAll("\\s", "");
        String[] hosts = connectedHostIpList.split(",");
        for (String host : hosts) {
          if (!host.isEmpty()
              && !connectionsData.containsKey(host)) {
            connectionsData.put(host, new ArrayList<DNContainerConnections>());
          }
        }
      }
    } catch (Exception e) {
      ;
    }
  }

  private String readFirstLine(Path file) throws IOException {
    try (BufferedReader reader =
        Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      return reader.readLine();
    }
  }

  /**
   * @return the connectionsData
   */
  public Map<String, List<DNContainerConnections>> getConnectionsData() {
    return connectionsData;
  }

  @Override
  public Map<String, List<DNContainerConnections>> collect() {

    buildConnectionDB();

    Set<String> savedConnectedHosts = new HashSet<String>(connectedHosts);

    buildUpdateData();

    if (!connectionsData.isEmpty()) {
      if (!savedConnectedHosts.containsAll(connectedHosts)
          || !connectedHosts.containsAll(savedConnectedHosts)) {
        saveConnectedHosts(connectedHosts);
      }
    }

    return connectionsData;
  }

  public void saveConnectedHosts(Set<String> connectedHosts) {
    try {
      Files.write(backupConnectedDNsFile,
          joiner.join(connectedHosts).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Cannot save the list of connected DataNodes", e);
    }
  }

  private void buildUpdateData() {
    try {
      lock.lock();
      connectedHosts.clear();
      // First collect all updated hosts
      Set<String> submitHostList =
          new HashSet<String>(connectionsData.keySet());
      connectedHosts.addAll(submitHostList);

      Map<String, NMContainerConnections> containersMap =
          connectionHandler.getContainersMap();
      List<NMContainerConnections> finishedContainers =
          connectionHandler.getFinishedContainers();

      for (NMContainerConnections nmContainers : finishedContainers) {
        nmContainers.collectUpdatedHosts(submitHostList, connectedHosts);
      }
      for (NMContainerConnections nmContainers : containersMap.values()) {
        nmContainers.collectUpdatedHosts(submitHostList, connectedHosts);
      }

      // track changes
      for (NMContainerConnections nmContainers : finishedContainers) {
        nmContainers.commitChanges(connectionsData, submitHostList);
      }

      for (NMContainerConnections nmContainers : containersMap.values()) {
        nmContainers.commitChanges(connectionsData, submitHostList);
      }

      finishedContainers.clear();
    } finally {
      lock.unlock();
    }
  }

  public void buildConnectionDB() {
    Map<String, Connection> connections = new HashMap<String, Connection>();
    Map<String, Set<String>> pidInodes = new HashMap<String, Set<String>>();
    // Collect first time
    collector.collectConnections(false, connections);
    //LOG.debug("connections-p1: " + connections);
    try {
      lock.lock();

      Map<String, NMContainerConnections> containersMap =
          connectionHandler.getContainersMap();
      // Get PID tree
      walkPIDTree(FactoryHelper.getInstance().getProcFsRoot());

      // Collect all inodes for each container.
      for (NMContainerConnections nmCon : containersMap.values()) {
        nmCon.collectInodes();
      }
      
      //LOG.debug("pidInodes: " + pidInodes);

      // Collect again!
      collector.collectConnections(false, connections);
      //LOG.debug("connections-p2: " + connections);
      collector.collectConnections(true, connections, pidInodes);
      //LOG.debug("connections-ss3: " + connections);

      // Group connections by containers
      for (NMContainerConnections nmCon : containersMap.values()) {
        nmCon.collectConnections(connections, pidInodes);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the procfsTree
   */
  public CommonProcessTree getProcfsTree(String pid) {
    return procfsTree.get(pid);
  }

  private void walkPIDTree(String procRootPath) {
    Set<String> oldPids = new HashSet<>(pidMap.keySet());

    boolean foundNewProcess = false;
    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get(procRootPath))) {
      for (Path pidDir : directoryStream) {
        if (Files.isDirectory(pidDir)) {
          final String pid = pidDir.getFileName().toString();
          Matcher pidMatcher = VALID_PID.matcher(pid);
          if (pidMatcher.matches()) {
            if (oldPids.contains(pid)) {
              oldPids.remove(pid);
              continue;
            }

            try {
              final String statContent = readFirstLine(pidDir.resolve("stat"));
              Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(statContent);
              if (m.find()) {
                String ppid = m.group(3);
                updateProcFsTree(pid, ppid, procfsTree);
                pidMap.put(pid, ppid);
              }
              foundNewProcess = true;
            } catch (IOException e) {
              ;
            } catch (Exception e) {
              LOG.error("Cannot walk the directory: "
                  + pidDir, e);
            }
          }
        }
      }
    } catch (
        IOException | SecurityException e) {
      LOG.error(e, e);
    }

    for (String pid : oldPids) {
      removeOldPid(pid, procfsTree);
      pidMap.remove(pid);
      connectionHandler.removeInvalidPid(pid);
    }

    if (foundNewProcess
        || !oldPids.isEmpty()) {
      connectionHandler.buildContainerTrees(procfsTree);
    }
  }

  public static void removeOldPid(final String pid,
      Map<String, CommonProcessTree> treeDB) {
    CommonProcessTree pTree = treeDB.get(pid);
    if (pTree != null) {
      String ppid = pTree.getParentPID();
      if (ppid != null
          && treeDB.get(ppid) != null) {
        treeDB.get(ppid).getChildren().remove(pTree);
      }
    }

    treeDB.remove(pid);
  }

  public static void updateProcFsTree(final String pid, final String ppid,
      Map<String, CommonProcessTree> treeDB) {

    if (!treeDB.containsKey(ppid)) {
      // future parent process
      treeDB.put(ppid, new CommonProcessTree(ppid, null));
    }

    if (!treeDB.containsKey(pid)) {
      treeDB.put(pid, new CommonProcessTree(pid, ppid));
    } else {// it is a parent of other process.
      treeDB.get(pid).setParentPID(ppid);
    }
    // Update parent
    treeDB.get(ppid).getChildren().add(treeDB.get(pid));
  }
}
