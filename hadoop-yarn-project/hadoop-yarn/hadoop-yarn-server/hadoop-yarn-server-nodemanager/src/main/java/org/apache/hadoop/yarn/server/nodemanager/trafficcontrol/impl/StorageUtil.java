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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.Connection;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view.DNContainerConnections;

import com.google.common.base.Strings;

/**
 * Helper class.
 *
 */
public final class StorageUtil {

  static final Log LOG = LogFactory.getLog(StorageUtil.class);
  static final Pattern TC_CONFIG_NAME_FORMAT = Pattern.compile("^__(.+)__$");

  private StorageUtil() {
    return;
  }

  /**
   * Encode the list of container connections of a DataNode to JSON format.
   * 
   * @param containerSnapshot
   *          list of containers have connections with the DataNode
   * @return JSON output
   */
  public static String encodeHostConnections(
      List<DNContainerConnections> containerSnapshot) {
    JSONObject obj = new JSONObject();
    obj.put("containers", containerSnapshot);

    return JSONValue.toJSONString(obj, JSONStyle.MAX_COMPRESS);
  }

  /**
   * Construct a list of containers (have at least one connection) from a JSON
   * content.
   * 
   * @param parser
   *          JSON parser
   * 
   * @param jsonContent
   *          JSON input
   * @return list of containers have connections with the DataNode
   */
  public static List<DNContainerConnections> decodeHostConnections(
      JSONParser parser, String jsonContent) {
    try {
      return buildHostConnections((JSONObject) parser.parse(jsonContent));
    } catch (ParseException e) {
      LOG.warn("Invalid json content: "
          + e.getMessage(), e);
    }

    return new ArrayList<DNContainerConnections>();
  }

  /**
   * Construct a list of containers (have at least one connection) from a JSON
   * content.
   * 
   * @param parser
   *          JSON parser
   * 
   * @param reader
   *          reader of JSON input
   * @return list of containers have connections with the DataNode
   */
  public static List<DNContainerConnections> decodeHostConnections(
      JSONParser parser, Reader reader) {
    try {
      return buildHostConnections((JSONObject) parser.parse(reader));
    } catch (ParseException e) {
      LOG.warn("Invalid json content: "
          + e.getMessage(), e);
    }

    return new ArrayList<DNContainerConnections>();
  }

  /**
   * Return a list of containers have at least one connection with DN.
   * 
   * @param obj
   *          Json data
   * @return list of containers
   */
  private static List<DNContainerConnections> buildHostConnections(
      JSONObject obj) {
    List<DNContainerConnections> containers =
        new ArrayList<DNContainerConnections>();
    try {
      JSONArray containersObj = (JSONArray) obj.get("containers");
      for (int i = 0; i < containersObj.size(); i++) {
        JSONObject containerObj = (JSONObject) containersObj.get(i);
        String clsId = (String) containerObj.get("containerId");
        String rate = (String) containerObj.get("rate");
        DNContainerConnections container =
            new DNContainerConnections(clsId, rate);

        JSONArray connectionsObj = (JSONArray) containerObj.get("connections");
        for (int j = 0; j < connectionsObj.size(); j++) {
          JSONObject connectionObj = (JSONObject) connectionsObj.get(j);

          String srcHost = (String) connectionObj.get("srcHost");
          Integer srcPort = (Integer) connectionObj.get("srcPort");
          String dstHost = (String) connectionObj.get("dstHost");
          Integer dstPort = (Integer) connectionObj.get("dstPort");

          container.getConnections().add(
              new Connection(srcHost, srcPort, dstHost, dstPort));
        }
        // Add only if it has at least one connection.
        if (!container.getConnections().isEmpty()) {
          containers.add(container);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error occured when parsing JSON output: "
          + e.getMessage(), e);
    }

    return containers;
  }

  public static void createTimestampFile(FileSystem fs, Path hdfsPath,
      Long timestamp) {
    try (FSDataOutputStream os = fs.create(hdfsPath, true);) {
      os.writeBytes(String.valueOf(timestamp));
    } catch (IOException e) {
      LOG.error("Can not create timestamp file at "
          + hdfsPath, e);
    }
  }

  public static long getTimestamp(FileSystem fs, Path timestampFile) {
    long ts = 0;
    try (FSDataInputStream is = fs.open(timestampFile);
        BufferedReader br =
            new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {

      final String line = br.readLine();
      if (line != null
          && !line.isEmpty()) {
        ts = Long.parseLong(line.trim());
      }
    } catch (
        IOException | NumberFormatException e) {
      ;
    }

    return ts;
  }

  public static String getHostName(String encodedHostName,
      Map<String, String> cacheNodeNames) {
    String nodeName = cacheNodeNames.get(encodedHostName);
    if (Strings.isNullOrEmpty(nodeName)) {
      nodeName = encodedHostName;
      Matcher m = TC_CONFIG_NAME_FORMAT.matcher(nodeName);
      boolean validTCConfig = m.find();
      if (validTCConfig) {
        nodeName = m.group(1);
        if (!Strings.isNullOrEmpty(nodeName)) {
          cacheNodeNames.put(encodedHostName, nodeName);
        }
      }

      nodeName = cacheNodeNames.get(encodedHostName);
    }
    return nodeName;
  }
}
