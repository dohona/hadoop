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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Executor class for LTC filter settings.
 *
 */
public class TCFilterExecutor {
  static final Log LOG = LogFactory.getLog(TCFilterExecutor.class);

  static final String HASH_TABLE = "800";

  private int itemIndex;

  private int localPort;
  private String remoteHost;
  private int remotePort;

  private String device;
  private String parent;
  private String flowId;
  private String handle;
  private int priority = 1;

  private boolean isEnable = false;

  private TCClassExecutor ownClass;

  TCFilterExecutor(String device, String parent, int itemIndex, String flowId,
      String handle, int priority) {
    this.device = device;
    this.parent = parent;
    this.itemIndex = itemIndex;
    this.flowId = flowId;
    this.priority = priority;
    this.handle = handle;
    if (handle != null) {
      isEnable = true;
    }
  }

  public TCFilterExecutor(String device, String parent, int itemIndex,
      String flowId) {
    this(device, parent, itemIndex, flowId, null, 1);
  }

  public void setOwnClass(TCClassExecutor ownClass) {
    this.ownClass = ownClass;
  }

  public TCClassExecutor getOwnClass() {
    return ownClass;
  }

  public void setMatch(int localPort, String remoteHost, int remotePort) {
    isEnable = true;
    this.localPort = localPort;
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
  }

  public boolean match(int localPort, String remoteHost, int remotePort) {
    return this.localPort == localPort
        && this.remoteHost.equals(remoteHost) && this.remotePort == remotePort;
  }

  private String protocolMatch() {
    if (remoteHost != null) {
      return String.format(
          "match ip sport %d 0xffff match ip dst %s match ip dport %d 0xffff",
          localPort, remoteHost, remotePort);
    } else {
      return String.format("match ip sport %d 0xffff match ip dport %d 0xffff",
          localPort, remotePort);
    }
  }

  public int getItemIndex() {
    return itemIndex;
  }

  private String getItem() {
    return TCHandlerUtils.convertToHex(itemIndex,
        TCHandlerUtils.TC_FILTER_BASE_MINOR);
  }

  public String getHandle() {
    if (handle != null) {
      return handle;
    }
    return String.format("%s::%s", HASH_TABLE, getItem());
  }

  public String getHt() {
    return HASH_TABLE;
  }

  public TCCommand addFilterCmd() {
    if (!isEnable) {
      return null;
    }

    TCCommand cmd = new TCCommand();
    cmd.add("filter add");
    cmd.add(String.format("dev %s parent %s:  prio %s", device, parent,
        priority));
    cmd.add(String.format("handle ::%s protocol ip u32 ht %s::", getItem(),
        getHt()));
    cmd.add(protocolMatch());
    cmd.add(String.format("flowid %s", flowId));
    LOG.debug(cmd.toString());
    return cmd;
  }

  public TCCommand delFilterCmd() {
    if (!isEnable) {
      return null;
    }

    TCCommand cmd = new TCCommand();
    cmd.add("filter del");
    cmd.add(String.format("dev %s parent %s:  prio %s", device, parent,
        priority));
    cmd.add(String.format("handle %s protocol ip u32", getHandle()));
    LOG.debug(cmd.toString());
    return cmd;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TCFilterExecutor) {
      TCFilterExecutor o = (TCFilterExecutor) obj;
      return localPort == o.localPort
          && remoteHost.equals(o.remoteHost) && remotePort == o.remotePort;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "[TC filter -> Device: %s, parent: %s, handle: %s, u32: %s]", device,
        parent, getHandle(), protocolMatch());
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }
}
