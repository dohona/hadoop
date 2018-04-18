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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event.TCFilterEvent;

/**
 * Executor class for LTC class settings.
 *
 */
public class TCClassExecutor {

  static final Log LOG = LogFactory.getLog(TCClassExecutor.class);

  private String parent;
  private int itemIndex;
  private String rate;
  private TrafficControlDeviceExecutor device;
  private String minor;
  private String burst;

  private List<TCFilterExecutor> filters;

  public TCClassExecutor(TrafficControlDeviceExecutor tcDevice, String parent,
      int itemIndex, String rate, String burst) {
    this(tcDevice, parent, itemIndex, null, rate, burst);
  }

  TCClassExecutor(TrafficControlDeviceExecutor tcDevice, String parent,
      int itemIndex, String minor, String rate, String burst) {
    this.device = tcDevice;
    this.parent = parent;
    this.itemIndex = itemIndex;
    this.rate = rate;
    this.burst = burst;
    filters = new ArrayList<TCFilterExecutor>();
    if (minor != null) {
      this.minor = minor;
    } else {
      this.minor = TCHandlerUtils.getClassMinorFromIndex(itemIndex);
    }
  }

  public String getClassIdentifier() {
    return String.format("%s:%s", parent, minor);
  }

  public String getRate() {
    return rate;
  }

  /**
   * @param rate
   *          the rate to set
   */
  public void setRate(String rate) {
    this.rate = rate;
  }

  private TCCommand addClassCmd(String action) {
    TCCommand cmd = new TCCommand();
    cmd.add(String.format("class %s dev %s parent %s: classid %s htb rate %s",
        action, device.getName(), parent, getClassIdentifier(), rate));
    if (this.burst != null) {
      cmd.add(String.format("burst %s cburst %s", burst, burst));
    }
    return cmd;
  }

  public int doAddClass(boolean newClassMinor) {
    int ret;
    TCCommand cmd;
    if (newClassMinor) {
      cmd = addClassCmd("add");
      ret = device.exec(cmd);
      if (ret == 0) {
        cmd = new TCCommand();
        cmd.add(String.format("qdisc add dev %s parent %s handle %s: sfq",
            device.getName(), getClassIdentifier(), minor));
        ret = device.exec(cmd);
      }
      if (ret != 0) {
        LOG.warn("Cannot add new class: "
            + cmd);
        device.freeClassItemIndex(itemIndex);
      } else {
        LOG.debug("Add new class: "
            + cmd);
      }
    } else {
      cmd = addClassCmd("change");
      ret = device.exec(cmd);
      if (ret != 0) {
        LOG.warn("Can not change class: "
            + cmd);
      } else {
        LOG.debug("Reuse class: "
            + cmd);
      }
    }
    return ret;
  }

  /**
   * Delete all filters belonged to this class. However the class is stored for
   * later use.
   * 
   * @return 0 if the deletion is success.
   */
  public int doDelClass() {
    int result = 0;
    for (TCFilterExecutor filter : filters) {
      TCCommand cmd = filter.delFilterCmd();
      result = device.exec(cmd);
      device.freeFilterItemIndex(filter.getItemIndex());
      device.removeFilterFromList(filter.getHandle());
    }

    device.freeClassItemIndex(itemIndex);
    filters.clear();
    return result;
  }

  public TCFilterExecutor addFilter(TCFilterEvent event) {
    int pItemIndex = device.getFreeFilterItem();
    TCFilterExecutor filter = new TCFilterExecutor(device.getName(), parent,
        pItemIndex, getClassIdentifier());
    filter.setMatch(event.getLocalPort(), event.getRemoteHost(),
        event.getRemotePort());
    if (device.exec(filter.addFilterCmd()) == 0) {
      LOG.debug("Success added filter "
          + filter.toString());
      filter.setOwnClass(this);
      filters.add(filter);
      return filter;
    } else {
      LOG.warn("Cannot execute command: "
          + filter.addFilterCmd());
      device.freeFilterItemIndex(pItemIndex);
    }
    return null;
  }

  public boolean removeFilter(TCFilterExecutor filter) {
    return filters.remove(filter);
  }

  @Override
  public String toString() {
    return String.format("[TrafficControlClass -> Device: %s, parent: %s,"
        + " classId: %s, rate: %s, burst: %s , cburst %s]", device.getName(),
        parent, getClassIdentifier(), rate, burst, burst);
  }
}
