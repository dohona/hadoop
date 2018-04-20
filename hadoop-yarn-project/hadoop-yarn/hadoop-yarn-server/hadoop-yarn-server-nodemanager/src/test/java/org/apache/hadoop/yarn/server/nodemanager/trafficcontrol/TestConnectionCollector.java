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

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.junit.Assert;
import org.junit.Test;

public class TestConnectionCollector {

  private String testIP = "10.10.0.116";

  @Test
  public void testParsingIPv6MappedFormat() {
    String ipAddr = "::ffff:"
        + testIP;
    Assert.assertTrue(ipAddr
        + " is IPv4 mapped",
        ConnectionCollector.checkAndConvertIPv6to4Address(ipAddr)
            .equals(testIP));
  }

  @Test
  public void testParsingIPv6CompatibleFormat() {
    String ipAddr = "::"
        + testIP;
    Assert.assertTrue(ipAddr
        + " is IPv4 compatible", ConnectionCollector
        .checkAndConvertIPv6to4Address(ipAddr).equals(testIP));
  }

  @Test
  public void testParsingIPv6to4Format() {
    String ipAddr = "2002:0a0a:0074::";
    Assert.assertTrue(ipAddr
        + " is IPv4 compatible", ConnectionCollector
        .checkAndConvertIPv6to4Address(ipAddr).equals(testIP));
  }

  @Test
  public void testParsingIPOfNetTCP() {
    String hexStr = "74000A0A";
    Assert.assertTrue(hexStr
        + " hex string should be converted to " + testIP, ConnectionCollector
        .toIPv4AddrString(hexStr).equals(testIP));
  }

  @Test
  public void testParsingIPOfNetTCP6() {
    String hexStr = "0000000000000000FFFF000074000A0A";
    String expectedIPv6 = "0000:0000:0000:0000:0000:FFFF:0A0A:0074";
    String convertedIP = ConnectionCollector.toIPv6AddrString(hexStr);
    Assert.assertTrue(hexStr
        + " hex string should be converted to " + expectedIPv6,
        convertedIP.equals(expectedIPv6));

    Assert.assertTrue("The IP should be  "
        + testIP, ConnectionCollector
        .checkAndConvertIPv6to4Address(convertedIP).equals(testIP));
  }

}
