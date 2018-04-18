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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class storing the connections between one NM container and one DN host.
 *
 */
public class DNContainerConnections {
  static final Log LOG = LogFactory.getLog(DNContainerConnections.class);

  private String containerId;
  private String rate;

  private List<Connection> connections;

  public DNContainerConnections(String containerId, String rate) {
    this.containerId = containerId;
    this.rate = rate;
    if (this.rate == null) {
      this.rate = "";
    }
    this.connections = new ArrayList<Connection>();
  }

  /**
   * @return the containerId
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * @return the rate
   */
  public String getRate() {
    return rate;
  }

  /**
   * @return the connections
   */
  public List<Connection> getConnections() {
    return connections;
  }

  public DNContainerConnections cloneContainer() {
    DNContainerConnections newOne =
        new DNContainerConnections(containerId, rate);
    for (Connection con : connections) {
      newOne.getConnections().add(con.cloneConnection());
    }

    return newOne;
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
        * result
        + ((containerId == null) ? 0 : containerId.hashCode());
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
    DNContainerConnections other = (DNContainerConnections) obj;
    if (containerId == null) {
      if (other.containerId != null) {
        return false;
      }
    } else if (!containerId.equals(other.containerId)) {
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
    return "DNContainerConnections [containerId="
        + containerId + ", rate=" + rate + ", connections=" + connections + "]";
  }
}
