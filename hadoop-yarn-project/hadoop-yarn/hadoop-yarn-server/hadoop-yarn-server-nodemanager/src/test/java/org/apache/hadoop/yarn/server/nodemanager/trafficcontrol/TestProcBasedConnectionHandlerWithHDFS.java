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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.BaseTestConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.HdfsTCDataSubmitter;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.StorageUtil;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestProcBasedConnectionHandlerWithHDFS extends
    BaseTestConnectionHandler {
  static final Log LOG = LogFactory
      .getLog(TestProcBasedConnectionHandlerWithHDFS.class);

  MiniDFSCluster cluster;
  FileSystem fs;

  Path hdfsDir;

  Path nm1PathOnDN1;
  Path nm1PathOnDN2;

  Path dnDir1;
  Path dnDir2;

  private void printContent(FileSystem fs, Path file) {
    try (FSDataInputStream is = fs.open(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
      String line;
      while ((line = br.readLine()) != null) {
        LOG.info(line);
      }
    } catch (IOException e) {
      LOG.info("can not get content", e);
    }
  }

  private List<DNContainerConnections> readContent(FileSystem fs, Path file) {
    try (FSDataInputStream is = fs.open(file)) {
      return StorageUtil.decodeHostConnections(parser,
          new InputStreamReader(is));
    } catch (IOException e) {
      LOG.info("can not get content", e);
    }

    return null;
  }

  private void listDir(FileSystem fs, Path dir) {
    FileStatus[] files;
    try {
      files = fs.listStatus(dir);
      for (FileStatus file : files) {
        LOG.info("LISTING: "
            + file.getPath().toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOG.info("can not list dir");
    }

  }

  @Override
  protected void initBackendStorage() {
    LOG.info("Mock HDFS cluster");
    conf = new HdfsConfiguration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      LOG.info("Get FileSystem");
      fs = cluster.getFileSystem();

      fs.mkdirs(new Path(HdfsTCDataSubmitter.TC_HDFS_DIR));
      fs.mkdirs(new Path(HdfsTCDataSubmitter.TC_HDFS_DIR, host1));
      fs.mkdirs(new Path(HdfsTCDataSubmitter.TC_HDFS_DIR, host2));

      hdfsDir = new Path(HdfsTCDataSubmitter.TC_HDFS_DIR);

      dnDir1 = new Path(HdfsTCDataSubmitter.TC_HDFS_DIR, host1);
      dnDir2 = new Path(HdfsTCDataSubmitter.TC_HDFS_DIR, host2);

      nm1PathOnDN1 = new Path(dnDir1, nm1TCConfigName);
      LOG.info("nm1ConfigOnDN1: "
          + nm1PathOnDN1);

      nm1PathOnDN2 = new Path(dnDir2, nm1TCConfigName);
      LOG.info("nm1ConfigOnDN2: "
          + nm1PathOnDN2);
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  @Override
  protected void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  protected void validateRound1() throws IOException {
    Assert.assertTrue(fs.exists(hdfsDir));
    listDir(fs, hdfsDir);

    // 0.0.0.181
    // |_ __0.0.0.181__
    LOG.info("dir1: "
        + dnDir1);

    Assert.assertTrue(fs.exists(dnDir1));
    listDir(fs, dnDir1);
    Assert.assertTrue(fs.exists(nm1PathOnDN1));

    // check content
    LOG.info("\nCheck content on "
        + nm1PathOnDN1);
    printContent(fs, nm1PathOnDN1);
    List<DNContainerConnections> dnContainers = readContent(fs, nm1PathOnDN1);
    Assert.assertTrue(dnContainers.size() == 1);
    DNContainerConnections dn = dnContainers.get(0);
    Assert.assertTrue(dn.getConnections().size() == 2);

    // 0.0.0.190
    // |_ 0.0.0.181
    Assert.assertTrue(fs.exists(dnDir2));
    Assert.assertTrue(fs.exists(nm1PathOnDN2));
    // check content
    LOG.info("Check content on tcConfig2");
    printContent(fs, nm1PathOnDN2);
    dnContainers = readContent(fs, nm1PathOnDN2);
    Assert.assertTrue(dnContainers.size() == 1);
    Assert.assertTrue(dnContainers.get(0).getConnections().size() == 1);

  }

  @Override
  protected void validateRound2() {
    // check content
    LOG.info("\nCheck content on tcConfig1\n");
    printContent(fs, nm1PathOnDN1);
    List<DNContainerConnections> dnContainers = readContent(fs, nm1PathOnDN1);
    Assert.assertTrue(dnContainers.size() == 1);
    Assert.assertTrue(dnContainers.get(0).getConnections().size() == 1);

    LOG.info("\nCheck content on tcConfig2\n");
    printContent(fs, nm1PathOnDN2);
    dnContainers = readContent(fs, nm1PathOnDN2);
    Assert.assertTrue(dnContainers.isEmpty());
  }

  @Override
  protected void validateRound3() {
    LOG.info("\nCheck content on tcConfig1\n");
    printContent(fs, nm1PathOnDN1);
    List<DNContainerConnections> dnContainers = readContent(fs, nm1PathOnDN1);
    Assert.assertTrue(dnContainers.isEmpty());

    LOG.info("\nCheck content on tcConfig2\n");
    printContent(fs, nm1PathOnDN2);
    dnContainers = readContent(fs, nm1PathOnDN2);
    Assert.assertTrue(dnContainers.isEmpty());

  }

}
