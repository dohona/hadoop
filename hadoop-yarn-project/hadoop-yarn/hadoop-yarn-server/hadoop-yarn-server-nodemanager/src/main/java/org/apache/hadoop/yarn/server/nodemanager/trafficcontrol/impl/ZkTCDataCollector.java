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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.minidev.json.parser.JSONParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractTCDataCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

import com.google.common.base.Strings;

/**
 * Retrieve connection data from ZooKeeper storage and report them to
 * {@link TrafficController} when there is a request from
 * {@link TrafficController}.
 *
 */
public class ZkTCDataCollector extends AbstractTCDataCollector {

  static final Log LOG = LogFactory.getLog(ZkTCDataCollector.class);
  private CuratorFramework zkClient;
  private PathChildrenCache cache = null;
  private JSONParser parser;
  private String nodeId;

  private volatile boolean isReady = false;

  private String dataRootPath;
  private Map<String, String> cachePaths = new HashMap<String, String>();

  private Map<String, List<DNContainerConnections>> updatedContainers =
      new HashMap<>();

  public ZkTCDataCollector(Configuration conf) {
    super(conf);
    String zkServerHostPort =
        conf.get(YarnConfiguration.NM_HDFS_BE_ZK_SERVER_ADDRESS, null);

    zkClient =
        CuratorFrameworkFactory.newClient(zkServerHostPort,
            new ExponentialBackoffRetry(1000, 5));

    parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
  }

  @Override
  public void initialize(String nodeId) {
    this.nodeId = nodeId;
    this.dataRootPath = ZKPaths.makePath(ZkTCDataSubmitter.ZK_ROOT, nodeId);
  }

  @Override
  public void start() {
    super.start();
    try {
      zkClient.start();

      // Check for root path and create if needed
      if (zkClient.checkExists().forPath(dataRootPath) == null) {
        zkClient.create().creatingParentsIfNeeded().inBackground()
            .forPath(dataRootPath, nodeId.getBytes(StandardCharsets.UTF_8));
      }

      // Start cache
      cache = new PathChildrenCache(zkClient, dataRootPath, true);
      cache.getListenable().addListener(new CollectorListener());
      cache.start(StartMode.POST_INITIALIZED_EVENT);
    } catch (Exception e) {
      LOG.error("Error happened: "
          + e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    CloseableUtils.closeQuietly(cache);
    CloseableUtils.closeQuietly(zkClient);
    super.stop();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * AbstractTCDataCollector#isReady()
   */
  @Override
  public boolean isReady() {
    return isReady;
  }

  private void processsEventData(ChildData data, boolean isRemoved) {
    final String nodePath = data.getPath();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling connections from "
          + nodePath);
    }
    synchronized (updatedContainers) {
      final String zkNodeName =
          StorageUtil
              .getHostName(ZKPaths.getNodeFromPath(nodePath), cachePaths);
      if (!Strings.isNullOrEmpty(zkNodeName)) {
        if (isRemoved) {
          updatedContainers.put(zkNodeName,
              new ArrayList<DNContainerConnections>());
        } else {
          updatedContainers.put(
              zkNodeName,
              StorageUtil.decodeHostConnections(parser,
                  new String(data.getData(), StandardCharsets.UTF_8)));
        }
      }
    }
  }

  @Override
  public Map<String, List<DNContainerConnections>> collectData() {
    Map<String, List<DNContainerConnections>> connectionMap = new HashMap<>();
    synchronized (updatedContainers) {

      Iterator<Entry<String, List<DNContainerConnections>>> entries =
          updatedContainers.entrySet().iterator();
      while (entries.hasNext()) {
        List<DNContainerConnections> list =
            new ArrayList<DNContainerConnections>();
        Entry<String, List<DNContainerConnections>> entry = entries.next();
        for (DNContainerConnections dnCon : entry.getValue()) {
          list.add(dnCon.cloneContainer());
        }
        connectionMap.put(entry.getKey(), list);
      }

      if (!updatedContainers.isEmpty()) {
        updatedContainers.clear();
      }
    }

    return connectionMap;
  }

  class CollectorListener implements PathChildrenCacheListener {

    @Override
    public void
        childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws Exception {

      switch (event.getType()) {
      case INITIALIZED: {
        LOG.info("Cache is populated!");
        isReady = true;
        break;
      }
      case CHILD_ADDED:
      case CHILD_UPDATED: {
        processsEventData(event.getData(), false);
        break;
      }
      case CHILD_REMOVED: {
        processsEventData(event.getData(), true);
        break;
      }
      default: {
        LOG.warn("An unexpected event occured: "
            + event.getType());
        break;
      }
      }
    }
  }
}
