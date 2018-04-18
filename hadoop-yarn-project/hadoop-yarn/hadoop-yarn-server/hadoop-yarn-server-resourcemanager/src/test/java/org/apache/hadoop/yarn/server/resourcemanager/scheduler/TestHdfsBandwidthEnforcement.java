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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

public class TestHdfsBandwidthEnforcement {
  private static final Log LOG = LogFactory
      .getLog(TestHdfsBandwidthEnforcement.class);

  private final int GB = 1024;

  private void printTestName(String testName) {
    LOG.info(String.format("**** %s ***", testName));
  }

  private void print(String type, Resource r) {
    LOG.info(String.format("%s <Memory: %d, vCores: %d, bandwidth: %f>", type,
        r.getMemory(), r.getVirtualCores(), r.getHdfsBandwidthEnforcement()));
  }

  @Test
  public void testAllocateRequest() {

    printTestName("Test Allocate Request");
    List<ResourceRequest> resourceAsk = new ArrayList<ResourceRequest>();

    float bandwidth = 10f;
    Resource resource = Resources.createResource(1024, 1, bandwidth);

    ResourceRequest rr = Records.newRecord(ResourceRequest.class);
    rr.setCapability(resource);

    print("ResourceRequest", rr.getCapability());

    resourceAsk.add(rr);

    AllocateRequest req =
        AllocateRequest.newInstance(0, 0, resourceAsk, null, null);

    AllocateRequestProto p = ((AllocateRequestPBImpl) req).getProto();
    req = new AllocateRequestPBImpl(p);

    List<ResourceRequest> ask = req.getAskList();

    Assert.assertEquals(ask.size(), 1);
    Assert.assertTrue(bandwidth == ask.get(0).getCapability()
        .getHdfsBandwidthEnforcement());

    print("ProtoBuffer ResourceRequest", ask.get(0).getCapability());

  }

  @Test
  public void testAllocateResponse() {
    printTestName(" Test Allocate Response");

    List<Container> containers = new ArrayList<Container>();

    float bandwidth = 10f;
    Resource resource = Resources.createResource(1024, 1, bandwidth);

    Container container =
        Container.newInstance(null, null, null, resource, null, null);

    print("ContainerResource", container.getResource());

    containers.add(container);

    AllocateResponse r =
        AllocateResponse.newInstance(3, new ArrayList<ContainerStatus>(),
            containers, new ArrayList<NodeReport>(), null, null, 3, null,
            new ArrayList<NMToken>(), null, null);

    // serde
    AllocateResponseProto p = ((AllocateResponsePBImpl) r).getProto();
    r = new AllocateResponsePBImpl(p);

    List<Container> allocatedContainers = r.getAllocatedContainers();
    Assert.assertTrue(allocatedContainers.size() == 1);
    Assert.assertTrue(allocatedContainers.get(0).getResource()
        .getHdfsBandwidthEnforcement() == bandwidth);

    print("ProtocolBuffer ContainerResource", allocatedContainers.get(0)
        .getResource());
  }

  @Test
  public void testNormalizeResourceUsingDefaultResourceCalculator() {
    printTestName("Test Normalize Resource Using DefaultResourceCalculator");

    ResourceCalculator rc = new DefaultResourceCalculator();
    final int minMemory = 1024;
    final int maxMemory = 8192;
    Resource minResource = Resources.createResource(minMemory, 0);
    Resource maxResource = Resources.createResource(maxMemory, 0);

    ResourceRequest ask = Records.newRecord(ResourceRequest.class);
    float bandwidth = 10f;
    Resource capability = Resources.createResource(minMemory, 1, bandwidth);

    ask.setCapability(capability);

    print("ResourceRequest", ask.getCapability());

    SchedulerUtils.normalizeRequest(ask, rc, null, minResource, maxResource);

    print("Normalized ResourceRequest", ask.getCapability());

    // DefaultResourceCalculator unset HDFS Bandwidth Enforcement
    Assert.assertTrue(0 == ask.getCapability().getHdfsBandwidthEnforcement());

  }

  @Test
  public void testNormalizeResourceUsingDominantResourceCalculator() {

    printTestName("Test Normalize Resource Using DominantResourceCalculator");

    ResourceCalculator rc = new DominantResourceCalculator();
    final int minMemory = 1024;
    final int maxMemory = 8192;
    Resource minResource = Resources.createResource(minMemory, 0);
    Resource maxResource = Resources.createResource(maxMemory, 0);

    ResourceRequest ask = Records.newRecord(ResourceRequest.class);
    float bandwidth = 10f;
    Resource capability = Resources.createResource(minMemory, 1, bandwidth);

    ask.setCapability(capability);

    print("ResourceRequest", ask.getCapability());

    SchedulerUtils.normalizeRequest(ask, rc, null, minResource, maxResource);

    print("Normalized ResourceRequest", ask.getCapability());

    Assert.assertTrue(bandwidth == ask.getCapability()
        .getHdfsBandwidthEnforcement());
  }

  @Test(timeout = 3000000)
  public void testContainerAllocationWithCapacityScheduler() throws Exception {

    printTestName("Test Container Allocation With Capacity Scheduler");
    YarnConfiguration conf = new YarnConfiguration();
    // use Capacity Scheduler
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // use Dominant Resource Calculator
    conf.setClass(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);

    try (MockRM rm = new MockRM(conf);) {
      rm.start();
      // Register node1
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 2 * GB, 4);
      MockNM nm2 = rm.registerNode("127.0.0.1:2234", 3 * GB, 4);

      nm1.nodeHeartbeat(true);
      nm2.nodeHeartbeat(true);

      // wait..
      int waitCount = 20;
      int size = rm.getRMContext().getRMNodes().size();
      while ((size = rm.getRMContext().getRMNodes().size()) != 2
          && waitCount-- > 0) {
        LOG.info("Waiting for node managers to register : "
            + size);
        Thread.sleep(100);
      }
      Assert.assertEquals(2, rm.getRMContext().getRMNodes().size());
      // Submit an application
      RMApp app1 = rm.submitApp(128);

      // kick the scheduling
      nm1.nodeHeartbeat(true);
      RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
      MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
      am1.registerAppAttempt();

      LOG.info("sending container requests ");

      float bandwidth = 10f;
      am1.addRequests(new String[] { "*" }, 3 * GB, 1, 1, null, bandwidth);

      List<ResourceRequest> requests = am1.getRequests();
      if (requests != null
          && requests.size() > 0) {
        print("ResourceRequest", requests.get(0).getCapability());
      } else {
        LOG.info("ERROR: can not get ResourceRequest");
      }

      AllocateResponse alloc1Response = am1.schedule(); // send the request

      // kick the scheduler
      nm1.nodeHeartbeat(true);
      int waitCounter = 20;
      LOG.info("heartbeating nm1");
      while (alloc1Response.getAllocatedContainers().size() < 1
          && waitCounter-- > 0) {
        LOG.info("Waiting for containers to be created for app 1...");
        Thread.sleep(500);
        alloc1Response = am1.schedule();
      }
      LOG.info("received container : "
          + alloc1Response.getAllocatedContainers().size());

      // No container should be allocated.
      // Internally it should not been reserved.
      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);

      LOG.info("heartbeating nm2");
      waitCounter = 20;
      nm2.nodeHeartbeat(true);
      while (alloc1Response.getAllocatedContainers().size() < 1
          && waitCounter-- > 0) {
        LOG.info("Waiting for containers to be created for app 1...");
        Thread.sleep(500);
        alloc1Response = am1.schedule();
      }
      LOG.info("received container : "
          + alloc1Response.getAllocatedContainers().size());
      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

      List<Container> allocatedContainers =
          alloc1Response.getAllocatedContainers();
      for (Container container : allocatedContainers) {
        LOG.info("Allocated container resource: "
            + container.getResource());
        if (container.getResource() != null) {
          float b = container.getResource().getHdfsBandwidthEnforcement();
          Assert.assertTrue(bandwidth == b);

          print("Allocated container resource", container.getResource());
        } else {
          LOG.info("Can not get resource of the container");
        }
      }

      rm.stop();
    }
  }
}
