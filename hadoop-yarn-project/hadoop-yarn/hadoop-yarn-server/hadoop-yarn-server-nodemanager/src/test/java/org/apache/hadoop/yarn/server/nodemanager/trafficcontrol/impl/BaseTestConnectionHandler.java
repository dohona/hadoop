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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.minidev.json.parser.JSONParser;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.CommonProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ContainerProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public abstract class BaseTestConnectionHandler {
  private static final Log LOG = LogFactory
      .getLog(BaseTestConnectionHandler.class);

  private Path procFsRootDir = null;

  protected JSONParser parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
  protected int hdfsPort = 50010;

  protected Configuration conf;
  ProcBasedConnectionHandler connectionHandler;

  protected Map<String, Connection> connectionList;
  protected Set<String> inodeList;
  protected ContainerProcessTree spyTree;
  protected ContainerId containerId = BuilderUtils.newContainerId(12345, 1, 0,
      1);

  @Mock
  ConnectionCollector connectionCollector;

  protected String host1 = "0.0.0.181", host2 = "0.0.0.190";
  protected String pid = "1234", ppid = "123";

  protected String nm1TCConfigName = String.format("__%s__", host1);
  protected String nm2TCConfigName = String.format("__%s__", host2);

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws IOException {
    Assume.assumeFalse(System.getProperty("os.name").toLowerCase()
        .startsWith("win"));

    connectionList = new HashMap<String, Connection>();
    connectionList.put("1", new Connection(host1, 12345, host1, hdfsPort));
    connectionList.put("2", new Connection(host1, 23456, host2, hdfsPort));
    connectionList.put("3", new Connection(host1, 45678, host1, hdfsPort));

    inodeList = new HashSet<String>(Arrays.asList("1", "2", "3", "4", "5"));
    spyTree =
        spy(new ContainerProcessTree(containerId.toString(),
            new CommonProcessTree(pid, ppid)));
    when(spyTree.getInodes()).thenReturn(inodeList, inodeList, inodeList,
        inodeList, inodeList, inodeList);

    procFsRootDir = getTempProcFsRootDir();
    initTempProcFsRootDir(procFsRootDir);
  }

  protected abstract void initBackendStorage();

  protected abstract void shutdown();

  protected abstract void validateRound1() throws IOException;

  protected abstract void validateRound2() throws IOException;

  protected abstract void validateRound3() throws IOException;

  @Test(timeout = 60000)
  public void testTcClass() {

    initBackendStorage();

    try {

      Thread.sleep(1000);

      LOG.info("init Monitor");
      conf.set(YarnConfiguration.NM_HDFS_BE_CONTAINER_PLUGINS,
          DummyContainerService.class.getName());
      connectionHandler = new ProcBasedConnectionHandler(conf);

      ProcBasedConnectionMonitor connectionMonitor =
          spy(new ProcBasedConnectionMonitor(conf));
      connectionMonitor.setCollector(connectionCollector);
      Path backupFile =
          procFsRootDir
              .resolve(ProcBasedConnectionMonitor.CONNECTED_DN_FILE_NAME);
      connectionMonitor.setBackupConnectedDNsFile(backupFile);
      connectionHandler.setConnectionMonitor(connectionMonitor);

      doNothing().when(connectionMonitor).buildConnectionDB();

      connectionHandler.initialize(host1);

      LOG.info("addTcClassViewr");
      NMContainerConnections tcClass =
          new NMContainerConnections(containerId.toString(), 10, hdfsPort);
      tcClass.setProcessTree(spyTree);

      connectionHandler.addTcClassView(tcClass);

      LOG.info("call update [1]");
      tcClass.collectConnections(connectionList);
      connectionHandler.process();

      validateRound1();
      Thread.sleep(1000);

      // round2
      LOG.info("call update [2]");
      connectionList.remove("2");
      connectionList.remove("3");
      tcClass.collectConnections(connectionList);
      connectionHandler.process();

      validateRound2();

      Thread.sleep(1000);

      // round 3: delete class
      LOG.info("call update [3]");
      tcClass.stopTrackContainer();
      connectionHandler.process();

      validateRound3();

      Thread.sleep(1000);

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      shutdown();
    }
  }

  @After
  public void tearDown() throws IOException {
    cleanTempProcFsRootDir(procFsRootDir);
  }

  public static Path getTempProcFsRootDir() {
    Path procFs =
        Paths.get(
            System.getProperty("test.build.data",
                System.getProperty("java.io.tmpdir", "target")),
            "hdfs_traffic_control");
    return procFs;
  }

  public static void initTempProcFsRootDir(Path procFs) throws IOException {
    FileUtils.deleteQuietly(procFs.toFile());
    Files.createDirectories(procFs);
    FactoryHelper.getInstance().setProcFsRoot(procFs.toString());
  }

  public static void cleanTempProcFsRootDir(Path procFs) throws IOException {
    FileUtils.deleteQuietly(procFs.toFile());
    FactoryHelper.getInstance().setProcFsRoot("/proc");
  }
}
