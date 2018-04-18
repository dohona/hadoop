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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.event;

import java.util.List;

/**
 * 
 * Define the type of LTC settings.
 *
 */
public enum TCEventType {

  ADD_FILTER("ADD_FILTER"), DEL_FILTER("DEL_FILTER"), ADD_CLASS("ADD_CLASS"),
  DEL_CLASS("DEL_CLASS"), CHANGE_CLASS("CHANGE_CLASS"), UNDEF("UNDEF");

  private static final TCEnumHelper<TCEventType> DB =
      new TCEnumHelper<TCEventType>(TCEventType.values());
  private String name;

  TCEventType(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

  public static List<String> getNames() {
    return DB.getNames();
  }

  public static List<String> getAllowedNames() {
    return getNames();
  }

  public static TCEventType getTCEventType(String name) {
    return DB.getElement(name.toUpperCase());
  }

}
