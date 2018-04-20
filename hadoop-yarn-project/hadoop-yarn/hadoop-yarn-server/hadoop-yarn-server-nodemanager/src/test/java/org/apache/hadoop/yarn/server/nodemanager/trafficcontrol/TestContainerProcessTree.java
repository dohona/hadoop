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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionMonitor;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.CommonProcessTree;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.NMContainerConnections;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestContainerProcessTree {

  private Map<String, CommonProcessTree> procFsTree = new HashMap<>();

  @Test
  public void testRegisterContainerWithExistedPid() {
    procFsTree.clear();

    String firstPid = "100";

    ProcBasedConnectionMonitor.updateProcFsTree(firstPid, "1", procFsTree);
    ProcBasedConnectionMonitor.updateProcFsTree("101", firstPid, procFsTree);
    ProcBasedConnectionMonitor.updateProcFsTree("1001", "101", procFsTree);
    ProcBasedConnectionMonitor.updateProcFsTree("1002", "101", procFsTree);
    ProcBasedConnectionMonitor.updateProcFsTree("102", firstPid, procFsTree);

    String containerId = "clsId_1";
    NMContainerConnections nmView =
        new NMContainerConnections(containerId, 30, 50010);

    boolean updated = false;
    Set<String> pidList;

    // We should have 5 pids for this container
    updated = nmView.updateProcessTree(procFsTree.get(firstPid));
    pidList = nmView.getProcessTree().getCurrentPIDList();
    Assert.assertTrue(updated);
    Assert.assertTrue(pidList.size() == 5);

    // NMContainer must remove invalid pids when the pids are not existed
    // anymore
    ProcBasedConnectionMonitor.removeOldPid("1001", procFsTree);
    updated = nmView.updateProcessTree(procFsTree.get(firstPid));
    pidList = nmView.getProcessTree().getCurrentPIDList();
    Assert.assertTrue(updated);
    Assert.assertTrue(pidList.size() == 4
        && !pidList.contains("1001"));

    // Removing pid 101 must also remove its children
    ProcBasedConnectionMonitor.removeOldPid("101", procFsTree);
    updated = nmView.updateProcessTree(procFsTree.get(firstPid));
    pidList = nmView.getProcessTree().getCurrentPIDList();
    Assert.assertTrue(updated);
    Assert.assertTrue("", pidList.size() == 2
        && pidList.contains(firstPid) && pidList.contains("102"));

    // Nothing changes
    updated = nmView.updateProcessTree(procFsTree.get(firstPid));
    Assert.assertFalse(updated);

    // New pids must be detected
    ProcBasedConnectionMonitor.updateProcFsTree("103", firstPid, procFsTree);
    ProcBasedConnectionMonitor.updateProcFsTree("2002", "102", procFsTree);
    updated = nmView.updateProcessTree(procFsTree.get(firstPid));
    pidList = nmView.getProcessTree().getCurrentPIDList();
    Assert.assertTrue(updated);
    Assert.assertTrue("", pidList.size() == 4
        && pidList.contains("2002") && pidList.contains("103"));
  }
}
