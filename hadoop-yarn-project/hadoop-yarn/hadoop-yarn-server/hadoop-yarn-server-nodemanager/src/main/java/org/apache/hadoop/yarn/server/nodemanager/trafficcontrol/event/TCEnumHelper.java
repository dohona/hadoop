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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * Helper class for custom enum type.
 *
 * @param <Type>
 *          custom type.
 */
public class TCEnumHelper<Type> {

  private List<String> names;

  private Map<String, Type> nameMap;

  public TCEnumHelper(Type[] elements) {

    names = new ArrayList<String>();
    nameMap = new HashMap<String, Type>();

    if (elements == null) {
      return;
    }
    for (Type element : elements) {
      names.add(element.toString());
      nameMap.put(element.toString().toUpperCase(), element);
    }
  }

  public List<String> getNames() {
    return (names);
  }

  public void putElement(String name, Type element) {
    nameMap.put(name, element);
  }

  public Type getElement(String name) {
    if (name == null) {
      return null;
    }
    return nameMap.get(name.toUpperCase());
  }

  public Type getElement(String name, Type defaultValue) {
    if (name == null) {
      return defaultValue;
    }

    Type value = nameMap.get(name.toUpperCase());
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

}
