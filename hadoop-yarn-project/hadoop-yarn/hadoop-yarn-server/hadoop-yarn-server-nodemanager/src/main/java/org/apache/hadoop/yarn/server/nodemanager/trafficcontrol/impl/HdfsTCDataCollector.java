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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.minidev.json.parser.JSONParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

import com.google.common.base.Strings;

/**
 * Collect TC settings from HDFS storages.
 *
 */
public class HdfsTCDataCollector extends AbstractTCDataCollector {

  static final Log LOG = LogFactory.getLog(HdfsTCDataCollector.class);

  private HashMap<String, Long> hostCheckedTimestamp;
  private FileSystem fs;
  private Path rootHdfsDir;
  private String configRootPath;

  private JSONParser parser;
  private Map<String, String> cachePaths = new HashMap<String, String>();

  public HdfsTCDataCollector(Configuration conf) throws IOException {
    super(conf);

    hostCheckedTimestamp = new HashMap<String, Long>();
    this.fs = FileSystem.get(conf);
    this.configRootPath =
        conf.get(YarnConfiguration.NM_HDFS_BE_CONFIG_ROOT_DIR,
            HdfsTCDataSubmitter.TC_HDFS_DIR);
    parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
  }

  public void initialize(String nodeId) {
    rootHdfsDir = new Path(configRootPath, nodeId);
    try {
      if (!fs.exists(rootHdfsDir)) {
        fs.mkdirs(rootHdfsDir);
      }
    } catch (IOException e) {
      LOG.warn("Cannot check folder: "
          + rootHdfsDir, e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * AbstractTCDataCollector#isReady()
   */
  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public synchronized Map<String, List<DNContainerConnections>> collectData() {
    Map<String, List<DNContainerConnections>> hostConnections = new HashMap<>();
    Set<String> deletedHosts =
        new HashSet<String>(hostCheckedTimestamp.keySet());
    try {
      FileStatus[] connectedHosts = fs.listStatus(rootHdfsDir);
      for (FileStatus nmHostStatus : connectedHosts) {
        if (!nmHostStatus.isFile()) {
          continue;
        }

        final Path tcConfigFile = nmHostStatus.getPath();
        String nmHostName =
            StorageUtil.getHostName(tcConfigFile.getName(), cachePaths);

        if (Strings.isNullOrEmpty(nmHostName)) {
          continue;
        }

        boolean needUpdate = false;
        long lastModifiedTime = nmHostStatus.getModificationTime();
        deletedHosts.remove(nmHostName);

        if (!hostCheckedTimestamp.containsKey(nmHostName)) {
          needUpdate = true;
        } else {
          Long preTime = hostCheckedTimestamp.get(nmHostName);
          needUpdate = lastModifiedTime > preTime;
        }

        if (needUpdate) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Handling connections from "
                + nmHostName);
          }
          try {
            FSDataInputStream is = fs.open(tcConfigFile);
            hostConnections.put(nmHostName, StorageUtil.decodeHostConnections(
                parser, new InputStreamReader(is, StandardCharsets.UTF_8)));

            hostCheckedTimestamp.put(nmHostName, lastModifiedTime);
          } catch (Exception e) {
            LOG.warn("Can not read "
                + nmHostStatus.getPath() + ". Caused by: " + e, e);
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    // Deleted hosts should be also passed to the handler (same as hosts with
    // the empty content)
    for (String nmHost : deletedHosts) {
      hostConnections.put(nmHost, new ArrayList<DNContainerConnections>());
    }

    return hostConnections;
  }
}
