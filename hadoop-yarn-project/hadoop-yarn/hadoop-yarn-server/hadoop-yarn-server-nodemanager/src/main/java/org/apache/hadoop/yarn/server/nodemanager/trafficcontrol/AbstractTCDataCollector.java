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

import org.apache.hadoop.conf.Configuration;

/**
 * Collect TC settings from back-end storages.
 *
 */
public abstract class AbstractTCDataCollector implements
    TrafficControlDataCollector, AbstractService {

  public AbstractTCDataCollector(Configuration conf) {
    return;
  }

  public void start() {
    return;
  }

  public void stop() {
    return;
  }

  /**
   * Indicate whether the plugin is ready for collecting data.
   * 
   * @return true if the plugin is ready
   */
  public abstract boolean isReady();
}
