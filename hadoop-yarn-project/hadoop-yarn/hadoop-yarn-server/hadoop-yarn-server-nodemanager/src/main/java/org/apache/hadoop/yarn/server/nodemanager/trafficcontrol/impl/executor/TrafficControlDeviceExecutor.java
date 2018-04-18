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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCClassEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCEventType;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCFilterEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TrafficControlEvent;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;

import com.google.common.annotations.VisibleForTesting;

/**
 * 
 * Executor class for network devices.
 *
 */
public class TrafficControlDeviceExecutor {

  private static final Log LOG =
      LogFactory.getLog(TrafficControlDeviceExecutor.class);

  private static final String DEFAULT_QDISC_HANDLE = "10";
  private static final String TC_QDISC_TYPE = "htb";

  public static final Pattern TC_ROOT_QDISC_SHOW =
      Pattern.compile("^qdisc\\s+htb\\s+([0-9a-fA-F]+):\\s+root");

  public static final Pattern TC_HDFS_CLASS_SHOW =
      Pattern.compile("^class\\s+htb\\s+([0-9a-fA-F]+:([0-9a-fA-F]+))\\s+(.+?)"
          + "rate\\s+([0-9]+(\\.\\d+)?)([mMkK]b?(it)?|B|b)\\s+");

  public static final Pattern TC_HDFS_FILTER_INFO =
      Pattern.compile("^filter\\s+protocol\\s+ip\\s+pref\\s+(\\d+)\\s+(.+)"
          + "fh\\s+([0-9a-fA-F]+::([0-9a-fA-F]+))\\s+(.+)"
          + "flowid\\s+([0-9a-fA-F]+:[0-9a-fA-F]+)");

  public static final Pattern TC_OTHER_FILTER_INFO =
      Pattern.compile("^filter\\s+(.+)fh\\s+([0-9a-fA-F]*)"
          + ":([0-9a-fA-F]*):([0-9a-fA-F]+)");

  public static final Pattern TC_COMMON_FILTER_INFO =
      Pattern.compile("^filter\\s+(.+)flowid\\s+([0-9a-fA-F]+:[0-9a-fA-F]+)");

  static final Pattern TC_FILTER_MATCH_PORT =
      Pattern.compile("\\s*match\\s+([0-9a-fA-F]{4})([0-9a-fA-F]{4})/ffffffff"
          + "\\s+at\\s+20");

  static final Pattern TC_FILTER_MATCH_IP =
      Pattern.compile("\\s*match\\s+([0-9a-fA-F]{2})([0-9a-fA-F]{2})"
          + "([0-9a-fA-F]{2})([0-9a-fA-F]{2})/ffffffff\\s+at\\s+16");

  private String deviceName;
  private String qDiscHandle;
  private List<Boolean> clsItemPool;
  private List<Boolean> filterItemPool;

  private boolean initialized = false;

  private FactoryHelper helper;

  // Helper map to quickly find TcClass by identifier
  private Map<String, TCClassExecutor> tcClasses;

  // key = filterHandle
  private Map<String, TCFilterExecutor> tcFilters;

  private Map<String, TCClassExecutor> oldTcClasses = new HashMap<>();
  private List<TCFilterExecutor> oldTcFilters = new ArrayList<>();
  private Set<Integer> reservedFilterItems = new HashSet<Integer>();
  private Set<Integer> reservedClassItems = new HashSet<Integer>();
  private Set<String> otherClassidList = new HashSet<String>();

  private boolean isLoopBack = false;
  private int monitoringPort;

  @VisibleForTesting
  private String burst;

  public TrafficControlDeviceExecutor(String deviceName, int monitoringPort,
      FactoryHelper helper) {
    this(deviceName, null, monitoringPort, helper);
  }

  public TrafficControlDeviceExecutor(String deviceName, String burst,
      int monitoringPort, FactoryHelper helper) {
    this.deviceName = deviceName;
    this.monitoringPort = monitoringPort;
    clsItemPool = new ArrayList<Boolean>();
    filterItemPool = new ArrayList<Boolean>();
    tcClasses = new HashMap<String, TCClassExecutor>();
    tcFilters = new HashMap<String, TCFilterExecutor>();
    if (this.deviceName.equals("lo")) {
      isLoopBack = true;
    }

    this.burst = burst;
    this.helper = helper;
  }

  public void initDevice() throws IOException {
    LOG.info("Initialize Qdisc for "
        + deviceName);
    TCCommand tcCommand = new TCCommand();
    tcCommand.add(String.format("qdisc show dev %s", deviceName));

    BufferedReader input = helper.getBufferedReader(tcCommand);
    String line;
    boolean hasContent = false;
    while ((line = input.readLine()) != null) {
      // Cannot find device
      if (line.contains("Cannot find device")) {
        throw new IOException("Cannot find device: "
            + deviceName);
      }

      hasContent = true;
      Matcher m = TC_ROOT_QDISC_SHOW.matcher(line);
      if (m.find()) {
        qDiscHandle = m.group(1);
        LOG.info("Found existing root qdisc with handler "
            + qDiscHandle + ". Reuse it!");
        collectHdfsTcClasses();
        if (!oldTcClasses.isEmpty()) {
          collectHdfsTcFilters();
        } else {
          initialized = true;
        }

        return;
      }
    }

    if (hasContent) {
      tcCommand.clear();
      tcCommand.add(String.format("qdisc del dev %s root", deviceName));
      if (exec(tcCommand) != 0) {
        LOG.warn("Cannot execute qdisc command for device "
            + deviceName + ": " + tcCommand);
      }
    }

    qDiscHandle = DEFAULT_QDISC_HANDLE;

    // not found any, create new
    tcCommand.clear();
    tcCommand.add(String.format("qdisc add dev %s root handle %s: %s",
        deviceName, qDiscHandle, TC_QDISC_TYPE));

    if (exec(tcCommand) != 0) {
      throw new IOException("Can not execute command "
          + tcCommand);
    }
  }

  /**
   * @return the initialized
   */
  public boolean isInitialized() {
    return initialized;
  }

  int exec(TCCommand cmd) {
    return helper.exec(cmd);
  }

  /**
   * Collect all maybe HDFS class with minor number is greater than 1000.
   * 
   * @throws IOException
   */
  private void collectHdfsTcClasses() throws IOException {
    TCCommand tcCommand = new TCCommand();
    tcCommand.add(
        String.format("class show dev %s parent %s:", deviceName, qDiscHandle));
    BufferedReader input = helper.getBufferedReader(tcCommand);
    String line;

    while ((line = input.readLine()) != null) {
      line = line.trim();
      LOG.debug(line);
      Matcher m = TC_HDFS_CLASS_SHOW.matcher(line);
      if (m.find()) {
        String classid = m.group(1);
        String classHexMinor = m.group(2);

        int minor = TCHandlerUtils.convertToDecimal(classHexMinor)
            - TCHandlerUtils.TC_CLASS_BASE_MINOR;
        if (minor < 0) {
          otherClassidList.add(classid);
          continue;
        } else {
          reservedClassItems.add(minor);
        }

        String unit = m.group(6).toLowerCase();
        String newUnit = unit;
        switch (unit) {
        case "mb":
        case "kb":
          newUnit += "ps";
          break;
        case "m":
        case "k":
          newUnit += "bps";
          break;
        case "b":
          newUnit = "bps";
          break;
        default:
          break;
        }

        String rate = String.format("%s%s", m.group(4), newUnit);
        oldTcClasses.put(classid, new TCClassExecutor(this, qDiscHandle, -1,
            classHexMinor, rate, burst));
      }
    }

    if (!otherClassidList.isEmpty()) {
      LOG.debug(String.format("Some TC classes %s may not be ours.",
          otherClassidList));
    }

    if (!oldTcClasses.isEmpty()) {
      LOG.debug(String.format("Old TC classes: %s", oldTcClasses));
    }
  }

  private void collectHdfsTcFilters() throws IOException {
    TCCommand tcCommand = new TCCommand();
    tcCommand.add(String.format("filter show dev %s parent %s:", deviceName,
        qDiscHandle));
    BufferedReader input = helper.getBufferedReader(tcCommand);
    String line;
    int filterMinor = -1;
    String currentFilterFlowid = null;
    boolean mayBeOurFilter = false;
    boolean foundIp = false;
    boolean foundHdfsPort = false;
    TCFilterExecutor tcFilterExecutor = null;
    while ((line = input.readLine()) != null) {
      line = line.trim();
      LOG.debug(line);
      if (line.startsWith("filter")) {
        if (tcFilterExecutor != null) {
          if (foundIp
              && foundHdfsPort) {
            LOG.debug("Add old filter: "
                + tcFilterExecutor);
            oldTcFilters.add(tcFilterExecutor);
          } else if (currentFilterFlowid != null) {
            oldTcClasses.remove(currentFilterFlowid);
          }
        }

        filterMinor = -1;
        tcFilterExecutor = null;
        foundIp = false;
        foundHdfsPort = false;
        currentFilterFlowid = null;
        mayBeOurFilter = false;
        Matcher m = TC_COMMON_FILTER_INFO.matcher(line);
        // Valid filter
        if (m.find()) {
          currentFilterFlowid = m.group(2);
          // Not our filter
          if (!oldTcClasses.containsKey(currentFilterFlowid)) {
            currentFilterFlowid = null;
            continue;
          }

          Matcher m1 = TC_HDFS_FILTER_INFO.matcher(line);
          // May be our filter
          if (m1.find()) {
            mayBeOurFilter = true;
            filterMinor = TCHandlerUtils.convertToDecimal(m1.group(4))
                - TCHandlerUtils.TC_FILTER_BASE_MINOR;
            if (filterMinor >= 0) {
              reservedFilterItems.add(filterMinor);
              tcFilterExecutor = new TCFilterExecutor(deviceName, qDiscHandle,
                  -1, currentFilterFlowid, m1.group(3),
                  Integer.parseInt(m1.group(1)));
              LOG.debug("Found old filter: "
                  + tcFilterExecutor);
            }
          } else {
            currentFilterFlowid = null;
            Matcher m2 = TC_OTHER_FILTER_INFO.matcher(line);
            if (m2.find()) {
              filterMinor = TCHandlerUtils.convertToDecimal(m2.group(4))
                  - TCHandlerUtils.TC_FILTER_BASE_MINOR;
              if (filterMinor >= 0) {
                reservedFilterItems.add(filterMinor);
              }
            }
          }
        }
      } else if (line.startsWith("match")
          && mayBeOurFilter) {
        Matcher m3 = TC_FILTER_MATCH_PORT.matcher(line);
        if (m3.matches()) {
          if (Integer.parseInt(m3.group(1), 16) == monitoringPort ||
              Integer.parseInt(m3.group(2), 16) == monitoringPort) {
            foundHdfsPort = true;
          }
        } else {
          Matcher m4 = TC_FILTER_MATCH_IP.matcher(line);
          if (m4.find()) {
            foundIp = true;
          }
        }
      }
    }

    if (tcFilterExecutor != null) {
      if (foundIp
          && foundHdfsPort) {
        oldTcFilters.add(tcFilterExecutor);
      } else if (currentFilterFlowid != null) {
        oldTcClasses.remove(currentFilterFlowid);
      }
    }

    if (!oldTcFilters.isEmpty()) {
      LOG.debug("Old tc filters: "
          + oldTcFilters);
    }
  }

  private void removeOldTCSettings() {
    LOG.debug("Remove old tc settings of "
        + deviceName);
    TCCommand tcCommand = new TCCommand();
    for (TCFilterExecutor tcFilter : oldTcFilters) {
      tcCommand = tcFilter.delFilterCmd();
      if (exec(tcCommand) != 0) {
        LOG.warn("Can not execute command "
            + tcCommand);
      }
    }

    for (Entry<String, TCClassExecutor> entry : oldTcClasses.entrySet()) {
      tcCommand.clear();
      tcCommand.add(String.format("class del dev %s classid %s", deviceName,
          entry.getKey()));
      LOG.debug("Delete old LTC class: "
          + tcCommand);
      if (exec(tcCommand) != 0) {
        LOG.warn("Can not execute command "
            + tcCommand);
      }
    }

    // oldTcClasses.clear();
    // oldTcFilters.clear();
    initialized = true;
    LOG.debug("Remove old tc settings - DONE for "
        + deviceName);
  }

  /**
   * @return the deviceName
   */
  public String getName() {
    return deviceName;
  }

  int getFreeFilterItem() {
    return firstFreeItemIndex(filterItemPool, reservedFilterItems);
  }

  private int firstFreeItemIndex(List<Boolean> itemPool,
      Set<Integer> reservedIndexes) {
    int i;
    for (i = 0; i < itemPool.size(); i++) {
      if (itemPool.get(i)) {
        break;
      }
    }

    if (i == itemPool.size()) {
      if (reservedIndexes.contains(i)) {
        reservedIndexes.remove(i);
        itemPool.add(false);
        i++;
      }
      itemPool.add(false);
    } else {
      itemPool.set(i, false);
    }

    return i;
  }

  private void freeItem(List<Boolean> itemPool, int i) {
    if (i >= 0) {
      itemPool.set(i, true);
    }
  }

  void freeClassItemIndex(int i) {
    freeItem(clsItemPool, i);
  }

  void freeFilterItemIndex(int i) {
    freeItem(filterItemPool, i);
  }

  public void update(List<TrafficControlEvent> events) {
    if (!initialized) {
      try {
        removeOldTCSettings();
      } catch (Exception e) {
        LOG.error("Error occured when removing old tc settings", e);
      }
    }
    for (TrafficControlEvent event : events) {
      if (canApply(event)) {
        update(event);
      }
    }
  }

  private boolean canApply(TrafficControlEvent event) {
      return (event.isLoopback() == isLoopBack)
          || event.isLoopback();
	  //return true;
  }

  private void update(TrafficControlEvent event) {
    if (event.getEventType() == TCEventType.ADD_FILTER) {	
      doAddFilter((TCFilterEvent) event);
    } else if (event.getEventType() == TCEventType.DEL_FILTER) {
      doDelFilter((TCFilterEvent) event);
    } else if (event.getEventType() == TCEventType.ADD_CLASS) {
      doAddClass((TCClassEvent) event);
    } else if (event.getEventType() == TCEventType.DEL_CLASS) {
      doDelClass((TCClassEvent) event);
    } else if (event.getEventType() == TCEventType.CHANGE_CLASS) {
      doChangeClass((TCClassEvent) event);
    }
  }

  private void doAddClass(TCClassEvent event) {
    final String clsId = event.getContainerId();
    if (!tcClasses.containsKey(clsId)) {
      // get class id
      int i;
      boolean newClassMinor = false;
      for (i = 0; i < clsItemPool.size(); i++) {
        if (clsItemPool.get(i)) {
          break;
        }
      }

      if (i == clsItemPool.size()) {
        if (reservedClassItems.contains(i)) {
          reservedClassItems.remove(i);
          clsItemPool.add(false);
          i++;
        }
        newClassMinor = true;
        clsItemPool.add(false);

      } else {
        clsItemPool.set(i, false);
      }

      TCClassExecutor cls =
          new TCClassExecutor(this, qDiscHandle, i, event.getRate(), burst);

      if (cls.doAddClass(newClassMinor) == 0) {
        tcClasses.put(clsId, cls);
        LOG.debug("ClassId: "
            + clsId + " -> add class " + cls.toString());
      } else {
        LOG.error("Failed to create tcClass");
        return;
      }
    }
  }

  private void doChangeClass(final TCClassEvent event) {
    final String clsId = event.getContainerId();
    final TCClassExecutor cls = tcClasses.get(clsId);

    if (cls != null) {
      String oldRate = cls.getRate();
      cls.setRate(event.getRate());
      if (cls.doAddClass(false) == 0) {
        LOG.debug("ClassId: "
            + clsId + " -> change rate from  " + oldRate + " to: "
            + event.getRate());
      } else {
        LOG.error("Failed to change rate of tcClass");
      }
    }
  }

  private void doDelClass(TCClassEvent event) {
    final String containerId = event.getContainerId();
    final TCClassExecutor cls = tcClasses.get(containerId);
    if (cls != null) {
      if (cls.doDelClass() == 0) {
        LOG.debug("ClassId"
            + containerId + " -> del class " + cls.toString());
      }
      tcClasses.remove(containerId);
    }
  }

  private void doAddFilter(TCFilterEvent event) {
    final String clsId = event.getContainerId();
    TCClassExecutor cls = tcClasses.get(clsId);
    if (cls != null) {
      TCFilterExecutor filter = cls.addFilter(event);
      if (filter != null) {
        tcFilters.put(filter.getHandle(), filter);
      }
    }
  }

  private void doDelFilter(TCFilterEvent event) {
    TCFilterExecutor filter = findFilter(event);
    if (filter == null) {
      return;
    }

    TCCommand cmd = filter.delFilterCmd();
    if (exec(cmd) == 0) {
      filter.getOwnClass().removeFilter(filter);
      freeFilterItemIndex(filter.getItemIndex());
      tcFilters.remove(filter.getHandle());
    } else {
      LOG.warn("Cannot del filter: "
          + filter.toString());
    }
  }

  private TCFilterExecutor findFilter(TCFilterEvent event) {
    for (TCFilterExecutor filter : tcFilters.values()) {
      if (filter.match(event.getLocalPort(), event.getRemoteHost(),
          event.getRemotePort())) {
        return filter;
      }
    }
    return null;
  }

  void removeFilterFromList(String filterHandler) {
    tcFilters.remove(filterHandler);
  }
}
