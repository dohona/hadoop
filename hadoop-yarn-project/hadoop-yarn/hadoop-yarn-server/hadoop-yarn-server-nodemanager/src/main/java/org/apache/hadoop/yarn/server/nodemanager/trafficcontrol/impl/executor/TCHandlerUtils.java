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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.executor;

/**
 * Utilities for handling LTC.
 *
 */
public final class TCHandlerUtils {

  static final int TC_CLASS_BASE_MINOR = 1000;
  static final int TC_FILTER_BASE_MINOR = 1000;

  private TCHandlerUtils() {
  }

  public static String getClassMinorFromIndex(int index) {
    return convertToHex(index, TC_CLASS_BASE_MINOR);
  }

  public static TCCommand newTcCommand() {
    return new TCCommand();
  }

  public static String convertToHex(int itemIndex, int base) {
    return Integer.toHexString(itemIndex
        + base);
  }

  public static int convertToDecimal(String hexStr) {
    return Integer.parseInt(hexStr, 16);
  }
}
