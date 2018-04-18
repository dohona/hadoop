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

import java.util.HashSet;
import java.util.Set;

/**
 * Data class for storing the structure of /proc.
 *
 */
public class CommonProcessTree {
  private String pid;
  private String parentPID;

  private Set<CommonProcessTree> children = new HashSet<>();

  public CommonProcessTree(String pid, String ppid) {
    this.pid = pid;
    this.parentPID = ppid;
  }

  /**
   * @return the ppid
   */
  public String getParentPID() {
    return parentPID;
  }

  /**
   * @param ppid
   *          the ppid to set
   */
  public void setParentPID(String ppid) {
    this.parentPID = ppid;
  }

  /**
   * @return the pid
   */
  public String getPid() {
    return pid;
  }

  /**
   * @return the children
   */
  public Set<CommonProcessTree> getChildren() {
    return children;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime
        * result + ((pid == null) ? 0 : pid.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CommonProcessTree other = (CommonProcessTree) obj;
    if (pid == null) {
      if (other.pid != null) {
        return false;
      }
    } else if (!pid.equals(other.pid)) {
      return false;
    }
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "CommonProcessTree [pid="
        + pid + ", ppid=" + parentPID + ", children=" + children + "]";
  }
}
