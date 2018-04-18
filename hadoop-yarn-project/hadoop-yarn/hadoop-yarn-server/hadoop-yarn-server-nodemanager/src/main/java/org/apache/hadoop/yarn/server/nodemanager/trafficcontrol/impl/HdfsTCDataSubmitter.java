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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataSubmitter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

/**
 * This class submit TC settings to HDFS storage.
 *
 */
public class HdfsTCDataSubmitter extends AbstractTCDataSubmitter {

  static final Log LOG = LogFactory.getLog(HdfsTCDataSubmitter.class);

  public static final String TC_HDFS_DIR = "hdfs-bandwidth-enforcement";
  public static final String TC_CONFIG_FILE = "tconfig";
  public static final String TC_CONFIG_TIMESTAMP = "timestamp";

  private FileSystem fs;
  private String configRootPath = "";

  private String tcConfigName;

  private Set<String> connectedDatanodes = new HashSet<String>();

  public HdfsTCDataSubmitter(Configuration conf) throws IOException {
    super(conf);
    this.fs = FileSystem.get(conf);

    this.configRootPath =
        conf.get(YarnConfiguration.NM_HDFS_BE_CONFIG_ROOT_DIR, TC_HDFS_DIR);

    // Check and create a folder /user/{user_name}/hdfs-bandwidth-enforcement
    Path tcRootHdfsDir = new Path(configRootPath);
    if (!fs.exists(tcRootHdfsDir)) {
      if (!fs.mkdirs(tcRootHdfsDir)) {
        throw new IOException("Cannot create directory "
            + tcRootHdfsDir);
      }
    }
  }

  public void initialize(String localNodeId) {
    if (localNodeId != null) {
      tcConfigName = String.format("__%s__", localNodeId);
    }
  }

  public synchronized boolean submit(String remoteHost,
      List<DNContainerConnections> hostConnections) {

    try {
      String remoteHostName = NetUtils.getHostNameOfIP(remoteHost);
      if (remoteHostName == null) {
        LOG.warn("Cannot get hostname for "
            + remoteHost);
        remoteHostName = remoteHost;
      }

      // Only submit to HDFS if the dedicated DN already to collect it!
      Path remoteConfigPath = new Path(configRootPath, remoteHostName);
      if (!fs.exists(remoteConfigPath)) {
        LOG.warn(remoteConfigPath
            + " is not existed. Skip this round!");
        return false;
      }

      // config file
      Path tcConfig = new Path(remoteConfigPath, tcConfigName);
      LOG.info("Submitting to "
          + tcConfig);
      FSDataOutputStream os = fs.create(tcConfig, true);
      os.writeBytes(StorageUtil.encodeHostConnections(hostConnections));
      os.flush();
      os.close();

      connectedDatanodes.add(remoteHost);
      return true;
    } catch (IOException e) {
      LOG.error(e);
    }

    return false;
  }
}
