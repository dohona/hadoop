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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.BaseTestConnectionHandler;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestParsingProcNetTCP {

  private Joiner joiner = Joiner.on("  ").skipNulls();

  private Path procFsRootDir = null;

  private int hdfsPort = 50010;

  private Path procNetTCPPath;

  @Before
  public void setup() throws IOException {

    procFsRootDir = BaseTestConnectionHandler.getTempProcFsRootDir();
    BaseTestConnectionHandler.initTempProcFsRootDir(procFsRootDir);

    Path netPath = procFsRootDir.resolve("net");
    Files.createDirectory(netPath);
    procNetTCPPath = netPath.resolve("tcp");

    StringBuffer bf = new StringBuffer("");
    bf.append(joiner.join("  sl  local_address rem_address",
        "st tx_queue rx_queue tr tm->when retrnsmt", "uid  timeout inode\n"));
    bf.append(joiner.join("  0: 00000000:C396 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 14595 1 0000000000000000 100 0 0 10 0\n"));

    bf.append(joiner.join("  1: 00000000:0016 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "0        0 10095 1 0000000000000000 100 0 0 10 0\n"));

    bf.append(joiner.join("  2: 00000000:1F98 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 22466511 1 0000000000000000 100 0 0 10 0\n"));

    bf.append(joiner.join("    3: 00000000:27D8 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 26014 1 0000000000000000 100 0 0 10 0\n"));
    bf.append(joiner.join("    4: 00000000:34FA 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 22474669 1 0000000000000000 100 0 0 10 0\n"));
    bf.append(joiner.join("    5: 00000000:C35A 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 14858 1 0000000000000000 100 0 0 10 0\n"));
    bf.append(joiner.join("    6: 00000000:C39B 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 14889 1 0000000000000000 100 0 0 10 0\n"));

    bf.append(joiner.join("    19: 00000000:C3AA 00000000:0000 0A",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 11095 1 0000000000000000 100 0 0 10 0\n"));
    bf.append(joiner.join("    20: 74000A0A:2328 74000A0A:90B0 01",
        "00000000:00000000 02:000AFFB2 00000000",
        "1001        0 22486882 2 0000000000000000 20 4 27 10 7\n"));
    bf.append(joiner.join("   21: 74000A0A:2328 74000A0A:E34C 01",
        "00000000:00000000 02:000AD94B 00000000",
        "1001        0 17267 2 0000000000000000 20 4 27 10 -1 7\n"));
    bf.append(joiner.join("    23: 74000A0A:1F5F 74000A0A:A509 01",
        "00000000:00000000 02:000A8C7E 00000000",
        "1001        0 22474671 4 0000000000000000 20 4 33 10 7\n"));
    bf.append(joiner.join("    24: 74000A0A:C35A 74000A0A:BC1F 01",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 22483476 1 0000000000000000 20\n"));
    bf.append(joiner.join("    22: 74000A0A:A509 74000A0A:1F5F 01",
        "00000000:00000000 02:000A8C7E 00000000",
        "1001        0 22475090 3 0000000000000000 20 4 30 10 4 20 10 7\n"));
    bf.append(joiner.join("    25: 74000A0A:BC18 74000A0A:C35A 08",
        "00000000:00000001 00:00000000 00000000",
        "1001        0 22477456 1 0000000000000000 20 4 16 10 7\n"));
    bf.append(joiner.join("    26: 74000A0A:2328 74000A0A:9071 01",
        "00000000:00000000 02:000A8C7E 00000000",
        "1001        0 22474805 2 0000000000000000 22 4 29 10 7\n"));
    bf.append(joiner.join("    27: 74000A0A:0016 50000A0A:CD9B 01",
        "00001240:00000000 01:00000001 00000000",
        "0        0 22180097 4 0000000000000000 20 4 15 8 6\n"));
    bf.append(joiner.join("    28: 74000A0A:BC1B 74000A0A:C35A 08",
        "00000000:00000001 00:00000000 00000000",
        "1001        0 22477509 1 0000000000000000 20 4 16 10 7\n"));
    bf.append(joiner.join("    29: 74000A0A:9071 74000A0A:2328 01",
        "00000000:00000000 02:000A8C7E 00000000",
        "1001        0 22474154 2 0000000000000000 20 4 28 10 7\n"));
    bf.append(joiner.join("    30: 74000A0A:1F98 50000A0A:D07C 01",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 22486876 1 0000000000000000 20 4 30 39 20\n"));
    bf.append(joiner.join("    31: 74000A0A:0016 5A00000A:DC0D 01",
        "00000000:00000000 02:0008A618 00000000",
        "0        0 22443426 2 0000000000000000 21 4 17 10 -1\n"));
    bf.append(joiner.join("    32: 74000A0A:0016 50000A0A:CCF2 01",
        "00000000:00000000 02:0009F2E5 00000000",
        "0        0 22156060 2 0000000000000000 20 4 1 10 7\n"));
    bf.append(joiner.join("    33: 74000A0A:0016 5A00000A:DC0B 01",
        "00000000:00000000 02:00080C7E 00000000",
        "0        0 22438554 2 0000000000000000 21 4 23 10 -1\n"));
    bf.append(joiner.join("    34: 74000A0A:90AF 74000A0A:2328 06",
        "00000000:00000000 03:000000B2 00000000",
        "0        0 0 3 0000000000000000\n"));
    bf.append(joiner.join("    35: 74000A0A:0016 13000A0A:C833 01",
        "00000000:00000000 02:0008194B 00000000",
        "0        0 22438656 2 0000000000000000 22 4 9 18 17\n"));
    bf.append(joiner.join("    36: 74000A0A:BC1F 74000A0A:C35A 01",
        "00000000:00000000 00:00000000 00000000",
        "1001        0 22481365 1 0000000000000000 22 4 29 10 7\n"));
    bf.append(joiner.join("    37: 74000A0A:BC1C 74000A0A:C35A 08",
        "00000000:00000001 00:00000000 00000000",
        "1001        0 22482484 1 0000000000000000 21 4 20 10 7\n"));
    bf.append(joiner.join("    38: 74000A0A:90B0 74000A0A:2328 01",
        "00000000:00000000 02:000AFFB2 00000000",
        "1001        0 22488131 2 0000000000000000 20 4 30 10 7\n"));
    bf.append(joiner.join("    39: 74000A0A:E34C 74000A0A:2328 01",
        "00000000:00000000 02:000AD94B 00000000",
        "1001        0 14898 2 0000000000000000 20 4 30 10 -1\n"));

    bf.append(joiner.join("    40: 0000000000000000FFFF000074000A0A:0885",
        "0000000000000000FFFF000074000A0A:E78A 01",
        "00000000:00000000 00:00000000 00000000",
        "1001         0 171593978 1 0000000000000000 20 4 31 10 -1\n"));

    Files.write(procNetTCPPath, bf.toString().getBytes());
  }

  @Test
  public void parseProcNetTCPForOutgoingConnections() throws IOException {
    ConnectionCollector collector = new ConnectionCollector(hdfsPort, false);
    Map<String, Connection> cons =
        collector.parseProcNetTCP(procNetTCPPath, false, hdfsPort);

    Assert.assertTrue("We have one HDFS connection", cons.size() == 1);
  }

  @Test
  public void parseProcNetTCPForIncommingConnections() throws IOException {
    ConnectionCollector collector = new ConnectionCollector(hdfsPort, true);
    Map<String, Connection> cons =
        collector.parseProcNetTCP(procNetTCPPath, true, hdfsPort);
    Assert.assertTrue("We have one HDFS connection", cons.size() == 1);
  }

  @After
  public void tearDown() throws IOException {
    BaseTestConnectionHandler.cleanTempProcFsRootDir(procFsRootDir);
  }
}
