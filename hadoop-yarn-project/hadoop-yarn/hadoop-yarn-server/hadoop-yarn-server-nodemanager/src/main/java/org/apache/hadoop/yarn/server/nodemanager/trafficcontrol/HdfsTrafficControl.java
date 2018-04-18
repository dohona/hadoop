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

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.TrafficController;

/**
 * Main class for executing as a standalon application.
 *
 */
public final class HdfsTrafficControl implements AbstractService {

  static final Log LOG = LogFactory.getLog(HdfsTrafficControl.class);

  private ProcBasedConnectionHandler connectionHandler;
  private TrafficController trafficController;

  private Configuration conf;

  private HdfsTrafficControl(boolean runExecutorOnly) throws IOException {
    if (!FactoryHelper.getInstance().isTCApplicable()) {
      return;
    }

    conf = new Configuration(true);
    conf.addResource("yarn-site.xml");

    trafficController = new TrafficController(conf);
    // controllerThread = new TrafficControllerTherad();

    if (!runExecutorOnly) {
      connectionHandler = new ProcBasedConnectionHandler(conf);
      // monitoringThread = new ConnectionHandlerTherad();
    } else {
      LOG.warn("Only start Traffic Control Executor!");
    }
  }

  public static void main(String[] args) throws IOException {
    boolean onlyExecutor = false;
    InetAddress iAddress = InetAddress.getLocalHost();
    // To get the Canonical host name
    String canonicalHostName = iAddress.getCanonicalHostName();
    LOG.info("Hostname is: "
        + canonicalHostName);

    if (args != null) {
      for (String arg : args) {
        if (arg.equalsIgnoreCase("-onlyExecutor")
            || arg.equalsIgnoreCase("onlyExecutor")) {
          onlyExecutor = true;
          break;
        }
      }
    }

    LOG.info("only start TCExecutor: "
        + onlyExecutor);

    HdfsTrafficControl app = new HdfsTrafficControl(onlyExecutor);
    app.initialize(canonicalHostName);
    app.start();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {

        shutdown();
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          ;
        }
      }
    });
  }

  @Override
  public void initialize(String localNodeId) {
    if (connectionHandler != null) {
      connectionHandler.initialize(localNodeId);
    }

    if (trafficController != null) {
      trafficController.initialize(localNodeId);
    }

    addShutdownHook();

  }

  @Override
  public void start() {
    if (connectionHandler != null) {
      connectionHandler.start();
    }

    if (trafficController != null) {
      trafficController.start();
    }
  }

  @Override
  public void stop() {
    if (connectionHandler != null) {
      connectionHandler.stop();
    }

    if (trafficController != null) {
      trafficController.stop();
    }
  }

  public void shutdown() {
    stop();
  }
}
