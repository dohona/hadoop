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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.BaseTestConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.DummyContainerService;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionMonitor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestContainerService {

  private Path procFsRootDir = null;

  @Mock
  FactoryHelper helper;

  @Mock
  AbstractTCDataSubmitter submitter;

  DummyContainerService containerService;

  Configuration conf;
  String cid1 = "cls1111";
  String cid2 = "cls2222";
  String invalidClsId = "23Invalid-";

  ProcBasedConnectionHandler connectionHandler;

  @Before
  public void setup() throws IOException {

    procFsRootDir = BaseTestConnectionHandler.getTempProcFsRootDir();
    BaseTestConnectionHandler.initTempProcFsRootDir(procFsRootDir);

    conf = spy(new Configuration());
    containerService = new DummyContainerService(conf);

    when(helper.isTCApplicable()).thenReturn(true, true, true);
    when(helper.getTCDataSubmitter(any(Configuration.class)))
        .thenReturn(submitter);

    connectionHandler = new ProcBasedConnectionHandler(conf, helper);
    connectionHandler.registerContainerService(containerService);
    connectionHandler
        .setConnectionMonitor(new ProcBasedConnectionMonitor(conf));
  }

  @Test
  public void testInvalidContainerShouldNotBeAdded() {
    boolean added = containerService.addMonitoringContainer(invalidClsId, 50);
    Assert.assertFalse("Container is not added", added);
  }

  @Test
  public void testContainerWithoutRateShouldNotBeAdded() {
    boolean added = containerService.addMonitoringContainer(cid1, 0);
    Assert.assertFalse("Container is not added", added);
  }

  @Test
  public void testAddValidContainer() {
    boolean added = containerService.addMonitoringContainer(cid1, 50);
    Assert.assertTrue("Container must be added", added);
    Assert.assertTrue(
        containerService.getUnModifableMapOfContainers().containsKey(cid1));
  }

  @Test
  public void testAddAlreadyAddedContainerByOther() {
    connectionHandler
        .addTcClassView(new NMContainerConnections(cid1, 50, 50010));

    boolean added = containerService.addMonitoringContainer(cid1, 50);
    Assert.assertFalse("Container cannot be added if the same"
        + " containerId is already registered by other frameworks", added);
    Assert
        .assertTrue(containerService.getUnModifableMapOfContainers().isEmpty());
  }

  @Test
  public void testAddTwiceValidContainerShouldFail() {
    containerService.addMonitoringContainer(cid1, 50);

    // Add twice
    boolean added = containerService.addMonitoringContainer(cid1, 50);
    Assert.assertFalse("Container cannot be added twice", added);
  }

  @Test
  public void testUpdateContainerRate() {
    containerService.addMonitoringContainer(cid1, 50);

    boolean updated = containerService.updateContainerRate(cid1, 70);
    Assert.assertTrue("Container cannot be added twice", updated);
  }

  @Test
  public void testRegisterPidOfNonAddedContainer() {
    boolean ok = containerService.registerPid(cid1, 12);
    Assert.assertFalse("Pid of none regsistered container cannot be added", ok);
  }

  @Test
  public void testRegisterPidWithInvalidData() {
    boolean ok = containerService.registerPid(invalidClsId, 12);
    Assert.assertFalse("Pid of an invalid container cannot be added", ok);

    ok = containerService.registerPid(cid1, 1);
    Assert.assertFalse("Invalid Pid cannot be added", ok);
  }

  @Test
  public void testRegisterPidOfAnAddedContainer() throws IOException {

    Files.createFile(procFsRootDir.resolve("12"));
    containerService.addMonitoringContainer(cid1, 50);

    boolean ok = containerService.registerPid(cid1, 12);
    Assert.assertTrue("Pid should be added", ok);

    ok = containerService.registerPid(cid1, 13);
    Assert.assertFalse("We cannot report pid twice for one container", ok);
  }

  @Test
  public void testStopMonitoringContainerByExternalPlugin() throws IOException {
    containerService.addMonitoringContainer(cid1, 50);
    Assert.assertTrue(
        containerService.getUnModifableMapOfContainers().containsKey(cid1));
    containerService.stopMonitoringContainer(cid1);
    Assert
        .assertTrue(containerService.getUnModifableMapOfContainers().isEmpty());
  }

  @Test
  public void testStopMonitoringContainerByConnectionHandler()
      throws IOException {
    containerService.addMonitoringContainer(cid1, 50);
    Assert.assertTrue(
        containerService.getUnModifableMapOfContainers().containsKey(cid1));

    connectionHandler.removeTcClassView(cid1);
    Assert
        .assertTrue(containerService.getUnModifableMapOfContainers().isEmpty());

    // It is safe to remove once more
    containerService.stopMonitoringContainer(cid1);
  }

  @After
  public void tearDown() throws IOException {
    BaseTestConnectionHandler.cleanTempProcFsRootDir(procFsRootDir);
  }
}
