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
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FileBasedYarnContainerReporter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.ProcBasedConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.YarnContainerService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestYarnContainerService {

  @Mock
  FactoryHelper helper;

  @Mock
  AbstractTCDataSubmitter submitter;

  ProcBasedConnectionHandler connectionHandler;
  YarnContainerService yarnContainerService;
  Configuration conf;
  String cid1 = "cls1111";

  float minRate = 40.0f;
  float maxRate = 100.0f;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();

    conf.set(YarnConfiguration.NM_HDFS_BE_ENABLE, "true");
    conf.set(YarnConfiguration.NM_HDFS_BE_MIN_RATE, Float.toString(minRate));
    conf.set(YarnConfiguration.NM_HDFS_BE_MAX_RATE, Float.toString(maxRate));

    when(helper.isTCApplicable()).thenReturn(true, true, true);
  }

  @Test
  public void testFeatureIsDisableInDefault() {
    conf.unset(YarnConfiguration.NM_HDFS_BE_ENABLE);
    yarnContainerService =
        YarnContainerService.loadYarnContainerService(conf, helper);
    Assert.assertNull("yarnContainerService is disable in default",
        yarnContainerService);
  }

  @Test
  public void testLoadYarnContainerServiceInEmbeddedMode() {

    yarnContainerService =
        YarnContainerService.loadYarnContainerService(conf, helper);

    Assert.assertNotNull("yarnContainerService should not be null",
        yarnContainerService);
    Assert.assertFalse(yarnContainerService.isClientMode());

    Assert.assertEquals(minRate, yarnContainerService.getMinRate(), 0.0f);
    Assert.assertEquals(maxRate, yarnContainerService.getMaxRate(), 0.0f);
  }

  @Test
  public void testRateShouldBeNormalized() throws IOException {

    yarnContainerService =
        YarnContainerService.loadYarnContainerService(conf, helper);
    when(helper.getTCDataSubmitter(any(Configuration.class))).thenReturn(
        submitter);
    connectionHandler = new ProcBasedConnectionHandler(conf, helper);
    connectionHandler.registerContainerService(yarnContainerService);

    yarnContainerService.addMonitoringContainer(cid1, 20);
    Assert.assertTrue(yarnContainerService.getUnModifableMapOfContainers().get(
        cid1) == minRate);
  }

  @Test
  public void
      testCannotLoadYarnContainerServiceInClientModeWithoutReportPlugin() {

    conf.set(YarnConfiguration.NM_HDFS_BE_CLIENT_MODE, "true");
    yarnContainerService =
        YarnContainerService.loadYarnContainerService(conf, helper);

    Assert.assertNull(
        "YarnContainerService need a report plugin in the client mode",
        yarnContainerService);
  }

  @Test
  public void testLoadYarnContainerServiceInClientModeWithReportPlugin() {

    conf.set(YarnConfiguration.NM_HDFS_BE_CLIENT_MODE, "true");
    conf.set(YarnConfiguration.NM_HDFS_BE_REPORT_SERVICE_CLASS,
        FileBasedYarnContainerReporter.class.getName());

    yarnContainerService =
        YarnContainerService.loadYarnContainerService(conf, helper);

    Assert.assertNotNull("YarnContainerService should be loaded",
        yarnContainerService);

    Assert.assertTrue(yarnContainerService.isClientMode());
  }
}
