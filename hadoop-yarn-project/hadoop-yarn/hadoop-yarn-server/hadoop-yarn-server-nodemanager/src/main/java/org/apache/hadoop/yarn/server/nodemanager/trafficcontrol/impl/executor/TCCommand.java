/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * Basic class for manipulating LTC commands.
 *
 */
public class TCCommand {

  private static final String[] TC_CMD = new String[] {"sudo", "tc"};

  private static Joiner joiner = Joiner.on(" ");

  private List<String> cmdList;

  public TCCommand() {
    cmdList = new ArrayList<String>(Arrays.asList(TC_CMD));
  }

  public void add(String cmd) {
    cmdList.add(cmd);
  }

  public void addAll(String[] arrays) {
    for (String s : arrays) {
      cmdList.add(s);
    }
  }

  public void clear() {
    cmdList.clear();
    cmdList.addAll(Arrays.asList(TC_CMD));
  }

  public String toString() {
    return joiner.join(cmdList);
  }
}
