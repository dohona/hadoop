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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

/**
 * Collects HDFS connections on host and submit to the persistent storage.
 * 
 */
public abstract class AbstractConnectionHandler implements ConnectionMonitor,
    TrafficControlDataSubmitter {

  static final Log LOG = LogFactory.getLog(AbstractConnectionHandler.class);

  public synchronized void process() {

    Map<String, List<DNContainerConnections>> partialConnections = collect();
    if (partialConnections.isEmpty()) {
      return;
    }

    long start = System.currentTimeMillis();
    for (Entry<String, List<DNContainerConnections>> entry : partialConnections
        .entrySet()) {
      submit(entry.getKey(), entry.getValue());
    }
    LOG.debug("submit time elapsed: "
        + (System.currentTimeMillis() - start) + " ms");
  }
}
