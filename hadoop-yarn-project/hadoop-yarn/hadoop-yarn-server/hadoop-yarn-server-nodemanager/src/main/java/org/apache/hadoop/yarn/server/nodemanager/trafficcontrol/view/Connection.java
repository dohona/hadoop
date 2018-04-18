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

/**
 * Data class for an IPv4 connection.
 *
 */
public class Connection {
  private String srcHost;
  private int srcPort;
  private String dstHost;
  private int dstPort;

  public Connection(String srcHost, int srcPort, String dstHost, int dstPort) {
    this.srcHost = srcHost;
    this.srcPort = srcPort;
    this.dstHost = dstHost;
    this.dstPort = dstPort;
  }

  public Connection reverseConnection() {
    return new Connection(dstHost, dstPort, srcHost, srcPort);
  }

  public Connection cloneConnection() {
    return new Connection(srcHost, srcPort, dstHost, dstPort);
  }

  /**
   * @return the srcHost
   */
  public String getSrcHost() {
    return srcHost;
  }

  /**
   * @return the srcPort
   */
  public int getSrcPort() {
    return srcPort;
  }

  /**
   * @return the dstHost
   */
  public String getDstHost() {
    return dstHost;
  }

  /**
   * @return the dstPort
   */
  public int getDstPort() {
    return dstPort;
  }

  public boolean isLoopback() {
    return srcHost.equals(dstHost);
  }

  public String formatConnection() {
    return String.format("%s:%d->%s:%d", srcHost, srcPort, dstHost, dstPort);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Connection) {
      Connection other = (Connection) obj;
      return this.srcHost.equals(other.srcHost)
          && this.srcPort == other.srcPort
          && this.dstHost.equals(other.dstHost)
          && this.dstPort == other.dstPort;
    }

    return false;
  }

  @Override
  public String toString() {
    return String.format("Connection: %s:%d->%s:%d", srcHost, srcPort, dstHost,
        dstPort);
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42;
  }
}
