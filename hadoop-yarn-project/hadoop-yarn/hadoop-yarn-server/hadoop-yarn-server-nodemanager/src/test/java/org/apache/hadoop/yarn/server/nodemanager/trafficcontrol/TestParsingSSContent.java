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

import java.util.regex.Matcher;

import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.ConnectionCollector;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestParsingSSContent {

  // 0 3 10.10.0.116:48843 10.10.0.116:50010 timer:(on,200ms,0)
  // users:(("java",14882,279)) uid:1001 ino:22626218 sk:ffff880199aedb00
  // <->
  // 0 0 10.10.0.116:48839 10.10.0.116:50010 users:(("java",19747,204))
  // uid:1001 ino:22626890 sk:ffff8804056ce900 <->
  // 0 0 10.10.0.116:50010 10.10.0.116:48805 users:(("java",2358,275))
  // uid:1001 ino:22618314 sk:ffff880199aef000 <->

  private Joiner joiner = Joiner.on("  ").skipNulls();

  @Test
  public void testParsingHeader() {
    String[] header =
        new String[] { "Recv-Q", "Send-Q", "Local", "Address:Port", "Peer",
            "Address:Port" };

    Matcher m = match(joiner.join(header));
    Assert.assertFalse("Header should be ignored", m.matches());
  }

  @Test
  public void testParsingFullSSLineUbuntu14() {
    String line =
        String.format("0  0  %s:%d  %s:%d\t%s %s %s ino:1234 %s",
            "10.10.0.116", 1234, "10.10.0.80", 50010, "timer:(on,200ms,0)",
            "users:((\"java\",14882,279))", "uid:1001",
            "sk:ffff880199aedb00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);
    Assert.assertTrue("The process id should be 14882",
        Integer.parseInt(m.group(7)) == 14882);

  }

  @Test
  public void testParsingFullSSLineUbuntu16() {
    String line =
        String.format("0  0  %s:%d  %s:%d\t%s %s %s ino:1234 %s",
            "10.10.0.116", 1234, "10.10.0.80", 50010,
            "users:((\"java\",pid=14882,fd=279),(\"java\",pid=14883,fd=279))",
            "timer:(on,200ms,0)", "uid:1001", "sk:ffff880199aedb00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);
    Assert.assertTrue("The process id should be 14882",
        Integer.parseInt(m.group(7)) == 14882);

  }

  @Test
  public void testParsingSSLineOfOtherUser() {
    String line =
        String.format("0  0  %s:%d  %s:%d\t %s ino:1234 %s", "10.10.0.116",
            1234, "10.10.0.80", 50010, "uid:1001", "sk:ffff880199aedb00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);

    Assert.assertTrue("Inode should be found!",
        Integer.parseInt(m.group(9)) == 1234);
  }

  @Test
  public void testParsingSSLineOfOtherUserWithIPv6() {
    String line =
        String.format("41503  0  %s:%d  %s:%d\t %s ino:1234 %s",
            "::ffff:10.10.0.116", 46361, "::10.10.0.116", 50010, "uid:1003",
            "sk:ffff880406143e00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);

    Assert.assertTrue("Inode should be found!",
        Integer.parseInt(m.group(9)) == 1234);
  }

  @Test
  public void testParsingPartialSSLineWithUsers() {
    String line =
        String.format("0  0  %s:%d  %s:%d\t%s %s ino:1234 %s", "10.10.0.116",
            1234, "10.10.0.80", 50010, "users:((\"java\",19747,204))",
            "uid:1001", "sk:ffff880199aedb00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);

    Assert.assertTrue("User id should be found!",
        Integer.parseInt(m.group(8)) == 1001);
  }

  @Test
  public void testParsingPartialSSLineWithTimers() {
    String line =
        String.format("0  0  %s:%d  %s:%d\t%s %s ino:1234 %s", "10.10.0.116",
            1234, "10.10.0.80", 50010, "timer:(on,200ms,0)", "uid:1001",
            "sk:ffff880199aedb00 <->");

    Matcher m = match(line);
    boolean find = m.matches();
    Assert.assertTrue(find);

    Assert.assertTrue("Dst port must be 50010",
        Integer.parseInt(m.group(4)) == 50010);
  }

  private Matcher match(String line) {
    return ConnectionCollector.SS_ESTABLISHED_LINE_FORMAT.matcher(line);
  }

}
