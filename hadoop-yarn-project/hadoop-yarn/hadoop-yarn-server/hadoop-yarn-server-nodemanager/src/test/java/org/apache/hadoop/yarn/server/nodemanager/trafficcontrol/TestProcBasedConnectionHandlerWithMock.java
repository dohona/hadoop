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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.AbstractContainerService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.BaseTestConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.DummyContainerService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionMonitor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.CommonProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ContainerProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mortbay.log.Log;

@RunWith(MockitoJUnitRunner.class)
public class TestProcBasedConnectionHandlerWithMock {

  Configuration conf;

  Path procFsRootDir = null;

  @Mock
  AbstractTCDataSubmitter submitter;

  @Mock
  FactoryHelper helper;

  @Mock
  ConnectionCollector connectionCollector;

  ProcBasedConnectionMonitor connectionMonitor;

  AbstractContainerService containerService;

  ProcBasedConnectionHandler connectionHandler;

  NMContainerConnections con;
  NMContainerConnections con1;

  ContainerProcessTree spyTree1;
  ContainerProcessTree spyTree2;

  Set<String> inodeList;
  Set<String> inodeList1;
  Map<String, Connection> connectionList;

  private String host1 = "0.0.0.181", host2 = "0.0.0.190";
  private String clsId1 = "cls1", clsId2 = "cls2", yarnPID1 = "1234",
      yarnPID2 = "1235";
  private int hdfsPort = 50010;

  @Before
  public void setup() throws IOException {
    procFsRootDir = BaseTestConnectionHandler.getTempProcFsRootDir();
    BaseTestConnectionHandler.initTempProcFsRootDir(procFsRootDir);

    Path yarnPID1Path = procFsRootDir.resolve(yarnPID1);
    Files.createFile(yarnPID1Path);
    Path yarnPID2Path = procFsRootDir.resolve(yarnPID2);
    Files.createFile(yarnPID2Path);

    conf = spy(new Configuration());

    when(helper.isTCApplicable()).thenReturn(true);
    when(helper.getTCDataSubmitter(any(Configuration.class))).thenReturn(
        submitter);

    connectionHandler = new ProcBasedConnectionHandler(conf, helper);
    containerService = new DummyContainerService(conf);
    connectionHandler.registerContainerService(containerService);

    // connectionCollector = spy(new ConnectionCollector(hdfsPort, false));
    connectionMonitor = spy(new ProcBasedConnectionMonitor(conf));
    connectionMonitor.setCollector(connectionCollector);
    Path backupFile =
        procFsRootDir
            .resolve(ProcBasedConnectionMonitor.CONNECTED_DN_FILE_NAME);
    connectionMonitor.setBackupConnectedDNsFile(backupFile);

    connectionHandler.setConnectionMonitor(connectionMonitor);

    inodeList = new HashSet<String>(Arrays.asList("1", "2", "3", "4", "5"));
    inodeList1 =
        new HashSet<String>(Arrays.asList("11", "12", "13", "14", "15"));

    spyTree1 =
        spy(new ContainerProcessTree(clsId1, new CommonProcessTree(yarnPID1,
            "2345")));
    spyTree2 =
        spy(new ContainerProcessTree(clsId2, new CommonProcessTree(yarnPID2,
            "22345")));

    con =
        new NMContainerConnections(clsId1, 60,
            connectionHandler.getMonitoringPort());
    con.setProcessTree(spyTree1);

    con1 =
        new NMContainerConnections(clsId2, 70,
            connectionHandler.getMonitoringPort());
    con1.setProcessTree(spyTree2);

    connectionList = new HashMap<String, Connection>();
    connectionList.put("1", new Connection(host1, 11111, host1, hdfsPort));
    connectionList.put("2", new Connection(host1, 11112, host1, hdfsPort));
    connectionList.put("5", new Connection(host1, 11115, host2, hdfsPort));

    when(spyTree1.getInodes()).thenReturn(inodeList);
    when(spyTree2.getInodes()).thenReturn(inodeList1);

    doNothing().when(connectionMonitor).buildConnectionDB();
  }

  @Test
  public void verifyCreation() {

    verify(helper, times(1)).getTCDataSubmitter(any(Configuration.class));

    assertTrue("Default monitor port is "
        + hdfsPort, connectionHandler.getMonitoringPort() == hdfsPort);
  }

  @Test
  public void nothingToSubmitWithNewEmptyContainer() {
    containerService.addMonitoringContainer(clsId1, 60);

    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();

    assertTrue("Submitter shouldn't see newly added empty containers",
        dns.isEmpty());

    verify(connectionMonitor, never()).saveConnectedHosts(
        anySetOf(String.class));

  }

  @Test
  public void updateTcClassViewThenCollect() {

    connectionHandler.addTcClassView(con);

    con.collectConnections(connectionList);

    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();

    assertTrue("We should have 2 datanodes", dns.keySet().size() == 2);
    assertTrue(dns.keySet().containsAll(Arrays.asList(host1, host2)));

    when(submitter.submit(anyString(), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    connectionHandler.process();
    verify(submitter, times(2)).submit(anyString(),
        anyListOf(DNContainerConnections.class));

    verify(connectionMonitor, times(1)).saveConnectedHosts(
        anySetOf(String.class));

    assertTrue("connectionsData cache is empty", dns.isEmpty());
  }

  @Test
  public void updateTcClassViewThenSubmitOK() {

    when(submitter.submit(anyString(), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    connectionHandler.addTcClassView(con);
    // Get the ref of connectionsData cache
    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();
    con.collectConnections(connectionList);

    connectionHandler.process();

    verify(submitter, times(2)).submit(anyString(),
        anyListOf(DNContainerConnections.class));
    assertTrue("connectionsData cache is empty", dns.isEmpty());
  }

  @Test
  public void submitTwoContainersTwiceAndAlwaysOK() {

    when(submitter.submit(eq(host1), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    when(submitter.submit(eq(host2), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    connectionHandler.addTcClassView(con);

    // 1. round
    con.collectConnections(connectionList);
    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();
    connectionHandler.process();

    assertTrue("connectionsData cache is empty", dns.isEmpty());

    // 2. round
    connectionHandler.addTcClassView(con1);
    connectionList.put("11", new Connection(host1, 11121, host1, hdfsPort));
    connectionList.put("12", new Connection(host1, 11122, host1, hdfsPort));
    // connectionList.put("15", new Connection(host1, 11125, host2, hdfsPort));

    con.collectConnections(connectionList);
    con1.collectConnections(connectionList);

    dns = connectionHandler.collect();

    System.out.println(dns);

    assertTrue("Changes are in "
        + host1 + " only", dns.size() == 1
        && dns.containsKey(host1));

    int consNum = 0;
    for (DNContainerConnections dn : dns.get(host1)) {
      consNum += dn.getConnections().size();
    }

    assertTrue(host1
        + " has 4 HDFS connections", consNum == 4);

    connectionHandler.process();
    assertTrue("connectionsData cache is empty", dns.isEmpty());
  }

  @Test
  public void submitTwoContainersTwiceWithSomeFailed() {

    when(submitter.submit(eq(host1), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    when(submitter.submit(eq(host2), anyListOf(DNContainerConnections.class)))
        .thenReturn(false, true);

    connectionHandler.addTcClassView(con);

    // 1. round
    con.collectConnections(connectionList);
    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();
    connectionHandler.process();

    assertTrue("connectionsData must contain "
        + host2, dns.size() == 1
        && dns.containsKey(host2));

    // 2. round
    connectionHandler.addTcClassView(con1);
    connectionList.put("11", new Connection(host1, 11121, host1, hdfsPort));
    connectionList.put("12", new Connection(host1, 11122, host1, hdfsPort));

    con.collectConnections(connectionList);
    con1.collectConnections(connectionList);

    dns = connectionHandler.collect();
    assertTrue("Changes are in both host", dns.size() == 2);

    int consNum = 0;
    for (DNContainerConnections dn : dns.get(host1)) {
      consNum += dn.getConnections().size();
    }

    assertTrue(host1
        + " has 4 HDFS connections", consNum == 4);

    assertTrue(host2
        + " has 1 HDFS connections", dns.get(host2).get(0).getConnections()
        .size() == 1);

    connectionHandler.process();
    assertTrue("connectionsData cache is empty", dns.isEmpty());
  }

  @Test
  public void removeDisconnectedHost() {

    when(submitter.submit(anyString(), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, true);

    connectionHandler.addTcClassView(con);

    // 1. update
    con.collectConnections(connectionList);
    connectionHandler.process();

    // 2. update
    connectionList.remove("5");
    con.collectConnections(connectionList);

    // Get the ref of connectionsData cache
    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();

    Log.info("dns: "
        + dns);
    assertTrue("connectionsData has "
        + host2 + " with empty list", dns.get(host2).isEmpty());
  }

  @Test
  public void updateTcClassViewThenSubmitFailed() throws IOException,
      InterruptedException {

    when(submitter.submit(anyString(), anyListOf(DNContainerConnections.class)))
        .thenReturn(true, false);

    connectionHandler.addTcClassView(con);
    // Get the ref of connectionsData cache
    Map<String, List<DNContainerConnections>> dns = connectionHandler.collect();
    con.collectConnections(connectionList);
    connectionHandler.process();

    assertTrue("connectionsData cache is not empty", dns.keySet().size() == 1);
  }

  @After
  public void tearDown() throws IOException {
    BaseTestConnectionHandler.cleanTempProcFsRootDir(procFsRootDir);
  }
}
