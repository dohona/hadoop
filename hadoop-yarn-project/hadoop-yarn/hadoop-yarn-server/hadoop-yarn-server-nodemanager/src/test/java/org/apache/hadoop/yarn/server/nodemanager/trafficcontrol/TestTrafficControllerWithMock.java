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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.TrafficController;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor.TCCommand;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor.TrafficControlDeviceExecutor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestTrafficControllerWithMock {
  Configuration conf;

  @Mock
  AbstractTCDataCollector collector;

  @Mock
  FactoryHelper helper;

  TrafficControlDeviceExecutor loopbackDevice;

  TrafficController tcController;

  private String host1 = "0.0.0.181";
  private String clsId1 = "cls1", clsId2 = "cls2";
  private int hdfsPort = 50010;

  @Before
  public void setup() throws IOException {

    conf = spy(new Configuration());

    loopbackDevice =
        spy(new TrafficControlDeviceExecutor("lo", hdfsPort, helper));

    when(helper.isTCApplicable()).thenReturn(true);
    when(helper.getTCDataCollector(any(Configuration.class))).thenReturn(
        collector);

    when(helper.getDevice(anyString(), anyInt(), any(FactoryHelper.class)))
        .thenReturn(loopbackDevice);

    tcController = new TrafficController(conf, false, helper, 0);
  }

  @Test
  public void testCreation() throws IOException {

    Assert.assertTrue("Default monitoring port is "
        + hdfsPort, hdfsPort == tcController.getMonitoringPort());
    verify(helper, times(1)).getDevice(eq("lo"), eq(hdfsPort), eq(helper));
    when(helper.getBufferedReader(any(TCCommand.class))).thenReturn(
        getQdiscShowOfLoopback());
    tcController.initialize(host1);
    Assert.assertTrue("Loopback device should be initialized properly",
        loopbackDevice.isInitialized());
  }

  @Test
  public void testCreationWithFail() throws IOException {

    when(helper.getBufferedReader(any(TCCommand.class))).thenReturn(
        new BufferedReader(new StringReader("Cannot find device \"eth1\"")));

    tcController.initialize(host1);
    Assert.assertFalse("Creation should be failed", tcController.isEnabled());
  }

  @Test
  public void testUpdateLoopback() throws IOException {
    when(helper.getBufferedReader(any(TCCommand.class))).thenReturn(
        getQdiscShowOfLoopback());
    tcController.initialize(host1);

    Map<String, List<DNContainerConnections>> connections = new HashMap<>();
    connections.put(host1, new ArrayList<DNContainerConnections>());

    DNContainerConnections dn1 = new DNContainerConnections(clsId1, "50mbps");
    dn1.getConnections().add(new Connection(host1, 11111, host1, hdfsPort));
    dn1.getConnections().add(new Connection(host1, 11112, host1, hdfsPort));

    when(collector.collectData()).thenReturn(connections);
    connections.get(host1).add(dn1);

    when(helper.exec(any(TCCommand.class))).thenReturn(0);

    tcController.update();

    // We should have 4 tc commands: 1 class, 1 qdisc and 2 filters
    verify(helper, times(4)).exec(any(TCCommand.class));

    // 2. round: Update container
    connections = new HashMap<>();
    dn1 = new DNContainerConnections(clsId1, "50mbps");
    dn1.getConnections().add(new Connection(host1, 11111, host1, hdfsPort));
    connections.put(host1, new ArrayList<DNContainerConnections>());
    connections.get(host1).add(dn1);

    when(collector.collectData()).thenReturn(connections);

    tcController.update();

    // 1 filter must be deleted
    verify(helper, times(5)).exec(any(TCCommand.class));

    // 3. round: remove container
    connections = new HashMap<>();
    connections.put(host1, new ArrayList<DNContainerConnections>());
    when(collector.collectData()).thenReturn(connections);
    tcController.update();

    // the last filter must be deleted, but the class is remained for latter
    verify(helper, times(6)).exec(any(TCCommand.class));

    // 4. round: New container
    DNContainerConnections dn2 = new DNContainerConnections(clsId2, "70mbps");
    dn2.getConnections().add(new Connection(host1, 11121, host1, hdfsPort));
    connections = new HashMap<>();
    connections.put(host1, new ArrayList<DNContainerConnections>());
    connections.get(host1).add(dn2);
    when(collector.collectData()).thenReturn(connections);
    tcController.update();

    // we should have more 2 tc events: change_class and add_filter
    verify(helper, times(8)).exec(any(TCCommand.class));

  }

  private BufferedReader getQdiscShowOfLoopback() {
    String s =
        "qdisc htb 10: root refcnt 2 r2q 10 "
            + " default 0 direct_packets_stat 30614827 direct_qlen 2\n"
            + "qdisc sfq 3e9: parent 10:3e9 limit 127p "
            + " quantum 64Kb depth 127 divisor 1024\n";
    return new BufferedReader(new StringReader(s));
  }
}
