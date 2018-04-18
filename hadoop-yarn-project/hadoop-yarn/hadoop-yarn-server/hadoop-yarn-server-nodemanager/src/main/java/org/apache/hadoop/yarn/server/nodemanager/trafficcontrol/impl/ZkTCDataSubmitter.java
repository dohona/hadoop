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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataSubmitter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * Submit connection data to ZooKeeper storage when there is a request from
 * {@link ProcBasedConnectionHandler}.
 *
 */
public class ZkTCDataSubmitter extends AbstractTCDataSubmitter {

  static final Log LOG = LogFactory.getLog(ZkTCDataSubmitter.class);
  public static final String ZK_ROOT = "tcData";

  private CuratorFramework zkClient;

  private String tcConfigName;

  public ZkTCDataSubmitter(Configuration conf) {
    super(conf);
    String zkServerHostPort =
        conf.get(YarnConfiguration.NM_HDFS_BE_ZK_SERVER_ADDRESS, null);

    zkClient =
        CuratorFrameworkFactory.newClient(zkServerHostPort,
            new ExponentialBackoffRetry(1000, 5));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter#start()
   */
  @Override
  public void start() {
    zkClient.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter#stop()
   */
  @Override
  public void stop() {
    CloseableUtils.closeQuietly(zkClient);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter
   * #initialize(org.apache.hadoop.yarn.server.nodemanager.Context)
   */
  @Override
  public void initialize(String localNodeId) {
    if (localNodeId != null) {
      tcConfigName = String.format("__%s__", localNodeId);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * TrafficControlDataSubmitter#submit(java.lang.String, java.util.List)
   */
  @Override
  public boolean submit(String remoteHost,
      List<DNContainerConnections> hostConnections) {
    try {

      String remoteHostName = NetUtils.getHostNameOfIP(remoteHost);
      if (remoteHostName == null) {
        LOG.warn("Cannot get hostname for "
            + remoteHost);
        remoteHostName = remoteHost;
      }

      final String remoteZKPath = ZKPaths.makePath(ZK_ROOT, remoteHostName);
      if (zkClient.checkExists().forPath(remoteZKPath) == null) {
        LOG.debug(remoteZKPath
            + " is not exist. Skip this round");
        return false;
      }

      final String zkPath = ZKPaths.makePath(remoteZKPath, tcConfigName);
      byte[] data =
          StorageUtil.encodeHostConnections(hostConnections).getBytes(
              StandardCharsets.UTF_8);
      try {
        zkClient.setData().forPath(zkPath, data);
      } catch (NoNodeException e) {
        zkClient.create().forPath(zkPath, data);
      }

      return true;
    } catch (Exception e) {
      LOG.error("Cannot submit data to "
          + remoteHost, e);
    }

    return false;
  }
}
