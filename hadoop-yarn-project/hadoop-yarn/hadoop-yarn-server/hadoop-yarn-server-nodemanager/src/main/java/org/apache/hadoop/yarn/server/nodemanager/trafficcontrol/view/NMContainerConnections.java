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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Class defined to handle connections of a given container and its child
 * processes. The TC events (class, filter) will be constructed based on the
 * collected connections.
 * 
 * E.g.: tc class add dev eth0 parent 1:1 classid 1:20 htb rate 3mbit
 * 
 * 
 */
public class NMContainerConnections {

  static final Log LOG = LogFactory.getLog(NMContainerConnections.class);

  private String containerId;
  private int monitoringPort;
  private String rate = null;
  private float rateInMbps = 0;

  private boolean initialized = false;

  private volatile boolean rateIsModified = false;

  // Cache all traced actual connections
  private List<Connection> connections;

  private Map<String, DNContainerConnections> dnConnectionsMap;
  private Set<String> updatedHostsList;

  private ContainerProcessTree processTree;

  private boolean isCompleted = false;
  private String myHostIp = null;

  public NMContainerConnections(String containerId, float rateInMbps,
      int port) {
    this.containerId = containerId;
    this.monitoringPort = port;

    connections = new ArrayList<Connection>();
    dnConnectionsMap = Maps.newHashMap();
    updatedHostsList = new HashSet<String>();
    setRate(rateInMbps);
  }

  public String getContainerId() {
    return containerId;
  }

  /**
   * @param processTree
   *          the processTree to set
   */
  public void setProcessTree(ContainerProcessTree processTree) {
    this.processTree = processTree;
    if (this.processTree != null) {
      this.processTree.buildPIDTree(false);
    }
  }

  public boolean updateProcessTree(CommonProcessTree commonProcessTree) {
    boolean hasChanges = false;
    if (commonProcessTree == null) {
      processTree = null;
      return false;
    }

    if (processTree == null
        || processTree.getProcessTree() == null
        || processTree.getProcessTree() != commonProcessTree) {
      processTree = new ContainerProcessTree(containerId, commonProcessTree);
      hasChanges = true;
    }

    final Set<String> oldPidsList =
        new HashSet<String>(processTree.getCurrentPIDList());

    processTree.buildPIDTree(false);

    if (!oldPidsList.containsAll(processTree.getCurrentPIDList())
        || !processTree.getCurrentPIDList().containsAll(oldPidsList)) {
      hasChanges = true;
    }
    if (hasChanges
        && LOG.isDebugEnabled()) {
      LOG.debug(containerId
          + ": pidsList: "
          + Joiner.on(",").join(processTree.getCurrentPIDList()));
    }
    return hasChanges;
  }

  /**
   * @return the rate
   */
  public String getRate() {
    return rate;
  }

  /**
   * @return the rateInMbps without units
   */
  public float getRateInMbps() {
    return rateInMbps;
  }

  /**
   * @param rateInMbps
   *          the rate to set
   */
  public boolean setRate(float rateInMbps) {
    synchronized (dnConnectionsMap) {
      if (rateInMbps > 0
          && this.rateInMbps != rateInMbps) {
        this.rateInMbps = rateInMbps;
        this.rate = rateInMbps
            + "mbps";
        rateIsModified = true;
        return true;
      }
    }
    return false;
  }

  /**
   * @return the isCompleted
   */
  public boolean isCompleted() {
    return isCompleted;
  }

  /**
   * @return the initialized
   */
  public boolean isInitialized() {
    return initialized;
  }

  /**
   * Indicate that the container is fully initialized.
   */
  public void initialized() {
    this.initialized = true;
  }

  public void collectInodes() {
    if (processTree != null) {
      processTree.collectInodes();
    }
  }

  @VisibleForTesting
  public void collectConnections(Map<String, Connection> allConnections) {
    collectConnections(allConnections, null);
  }

  /**
   * @return the processTree
   */
  public ContainerProcessTree getProcessTree() {
    return processTree;
  }

  public void collectConnections(Map<String, Connection> allConnections,
      Map<String, Set<String>> pidInodes) {
    List<Connection> myConnections = new ArrayList<Connection>();
    
    if (getProcessTree() == null) {
      if (initialized) {
        resetContainerConnections();
      }
      return;
    }

    if (rateInMbps <= 0
        || isCompleted) {
      return;
    }

    Set<String> inodes = getProcessTree().getInodes();
    if (pidInodes != null) {
      for (String pid : getProcessTree().getCurrentPIDList()) {
        if (pidInodes.containsKey(pid)) {
          inodes.addAll(pidInodes.get(pid));
        }
      }
    }

    for (String inode : inodes) {
      if (allConnections.containsKey(inode)) {
        myConnections.add(allConnections.get(inode).cloneConnection());
      }
    }
    
    syncConnections(myConnections);
  }

  private void syncConnections(List<Connection> parsedConnections) {
    List<Connection> currentConnections = new ArrayList<Connection>();
    
    synchronized (this.dnConnectionsMap) {
      Set<String> disconnectedHosts =
          new HashSet<String>(dnConnectionsMap.keySet());
      dnConnectionsMap.clear();
      for (Connection connection : parsedConnections) {
        final String remoteHost = connection.getDstHost();
        
        if(myHostIp == null) {
        	myHostIp = connection.getSrcHost();
        }
        
        disconnectedHosts.remove(remoteHost);
        disconnectedHosts.remove(myHostIp);

        if (!dnConnectionsMap.containsKey(remoteHost)) {
          dnConnectionsMap.put(remoteHost,
              new DNContainerConnections(containerId, rate));
        }
        
        if (!dnConnectionsMap.containsKey(myHostIp)) {
          dnConnectionsMap.put(myHostIp,
              new DNContainerConnections(containerId, rate));
        }

        // The connection is already traced
        if (connections.contains(connection)) {
          connections.remove(connection);
        } else {
          updatedHostsList.add(remoteHost);
          updatedHostsList.add(myHostIp);
          if (LOG.isDebugEnabled()) {
            LOG.debug(containerId
                + ": Detect new connection " + connection.formatConnection());
          }
        }

        dnConnectionsMap.get(remoteHost).getConnections()
            .add(connection.reverseConnection());
        
        dnConnectionsMap.get(myHostIp).getConnections()
            .add(connection);

        currentConnections.add(connection);
      }

      for (Connection disconnected : connections) {
        updatedHostsList.add(disconnected.getDstHost());
        updatedHostsList.add(myHostIp);
        if (LOG.isDebugEnabled()) {
          LOG.debug(containerId
              + ": Detect disconnected connection "
              + disconnected.formatConnection());
        }
      }

      connections = new ArrayList<Connection>(currentConnections);
      updatedHostsList.addAll(disconnectedHosts);
      if (rateIsModified) {
        rateIsModified = false;
        updatedHostsList.addAll(dnConnectionsMap.keySet());
        if (LOG.isDebugEnabled()) {
          LOG.debug(containerId
              + ": rate is modified, updatedHostsList: " + updatedHostsList);
        }
      }
    }
  }

  private void resetContainerConnections() {
    synchronized (this.dnConnectionsMap) {
      updatedHostsList = new HashSet<String>(dnConnectionsMap.keySet());
      dnConnectionsMap.clear();
      connections.clear();
    }
  }

  public void stopTrackContainer() {
    resetContainerConnections();
    isCompleted = true;
  }

  public void collectUpdatedHosts(Set<String> updatedHosts,
      Set<String> connectedHosts) {
    updatedHosts.addAll(updatedHostsList);
    connectedHosts.addAll(dnConnectionsMap.keySet());
  }

  public void commitChanges(Map<String, List<DNContainerConnections>> dnData,
      Set<String> submitHostList) {
    synchronized (this.dnConnectionsMap) {
      if(!submitHostList.isEmpty() && myHostIp!= null
          && !submitHostList.contains(myHostIp)) {
    	submitHostList.add(myHostIp);
      }
      
      for (String updatedDNHost : submitHostList) {
        if (!dnData.containsKey(updatedDNHost)) {
          dnData.put(updatedDNHost, new ArrayList<DNContainerConnections>());
        }

        DNContainerConnections updatedSnapshot =
            dnConnectionsMap.get(updatedDNHost);
        List<DNContainerConnections> list = dnData.get(updatedDNHost);

        // remove from cache
        if (updatedSnapshot == null
            || isCompleted) {
          list.remove(new DNContainerConnections(containerId, rate));
        } else {
          DNContainerConnections cloneSnapshot =
              updatedSnapshot.cloneContainer();
          int index = list.indexOf(updatedSnapshot);
          if (index > -1) {
            list.set(index, cloneSnapshot);
          } else {
            list.add(cloneSnapshot);
          }
        }
      }

      updatedHostsList.clear();
    }
  }

  public String toString() {
    return String.format(
        "[NMContainerConnections] clsId: %s, rate: %s, hdfsPort: %d]",
        containerId, rate, monitoringPort);
  }
}
