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

package org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.view;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;

import com.google.common.net.InetAddresses;

/**
 * This class is responsible for retrieve network data for processes.
 *
 */
public class ConnectionCollector {

  static final Log LOG = LogFactory.getLog(ConnectionCollector.class);

  public static final Pattern SS_ESTABLISHED_LINE_FORMAT_OLD = Pattern
      .compile("\\s*\\d+\\s+\\d+\\s+" // recv-send data
          + "([:0-9a-fA-F\\.]+):([0-9]+)\\s+" // source address
          + "([:0-9a-fA-F\\.]+):([0-9]+)\\s+" // destination address
          + "(timer:\\S+\\s+)?" // timer:(keepalive,120min,0)
          + "(users:\\(\\(.+,(\\d+),\\d+\\)\\)\\s+)?" //
          // users:(("java",14551,246))
          + "uid:(\\d+)\\s+" + "ino:(\\d+)" + ".+");
  
  public static final Pattern SS_ESTABLISHED_LINE_FORMAT = Pattern
	  .compile("\\s*\\d+\\s+\\d+\\s+" // recv-send data
	      + "([:0-9a-fA-F\\.]+):([0-9]+)\\s+" // source address
	      + "([:0-9a-fA-F\\.]+):([0-9]+)\\s+" // destination address
	      + "(users:\\(\\(.+,pid=(\\d+),fd=\\d+\\)\\)\\s+)?" //
	      // users:(("java",pid=14551,fd=246))
	      + "(timer:\\S+\\s+)?" // timer:(keepalive,120min,0)
	      + "uid:(\\d+)\\s+" + "ino:(\\d+)" + ".+");  

  public static final Pattern PROC_NET_TCP_LINE_FORMAT = Pattern
      .compile("\\s*\\d+:\\s+" // sl
          + "([0-9A-F]+):" // 4 bytes of local ip
          + "([0-9A-F]{4})" // Port of local ip
          + "\\s+([0-9A-F]+):" // 4 bytes of remote ip
          + "([0-9A-F]{4})" // Port of remote ip
          + "\\s+([0-9A-F]{2})" // state
          + "\\s+(\\d+):(\\d+)" // tx_queue:rx_queue
          + "\\s+(\\d+):(\\d+)" // tr:tm->when
          + "\\s+(\\d+)" // retrnsmt
          + "\\s+(\\d+)" // uid
          + "\\s+(\\d+)" // timeout
          + "\\s+(\\d+)" // inode
          + "(.+)");

  static final String[] STATES = new String[] {"UNDEF", "ESTABLISHED",
      "SYN_SENT", "SYN_RECV", "FIN_WAIT1", "FIN_WAIT2", "TIME_WAIT", "CLOSE",
      "CLOSE_WAIT", "LAST_ACK", "LISTEN", "CLOSING"};

  private String ssCommand;
  private int monitoringPort = 0;
  private boolean isDataNode = false;

  public ConnectionCollector(int monitoringPort, boolean isDataNode) {
    this.monitoringPort = monitoringPort;
    this.isDataNode = isDataNode;
    if (isDataNode) {
      ssCommand =
          String.format("ss -noept state established src *:%d", monitoringPort);
    } else {
      ssCommand =
          String.format("ss -noept state established dst *:%d", monitoringPort);
    }
    
    ssCommand = String.format("ss -noept state established src *:%d or dst *:%d", 
        monitoringPort, monitoringPort);

    LOG.debug("ss command: "
        + ssCommand);
  }

  public void sudoIsAvailabe() {
    if (isDataNode) {
      return;
    }
    ssCommand = String.format("sudo %s", ssCommand);
    LOG.debug("ss command: "
        + ssCommand);
  }

  /**
   * Collect connections from the /proc file system or the output of the ss
   * program.
   * 
   * @param useSS
   * @param connections
   * @param inodes
   */
  public void collectConnections(boolean useSS,
      Map<String, Connection> connections, Map<String, Set<String>> inodes) {
    if (useSS) {
      connections.putAll(readEstablishedConsWithSS(inodes));
    } else {
      connections.putAll(readProcNetTCP(FactoryHelper.getInstance()
          .getProcFsRoot()));
    }
  }

  /**
   * Collect all established from or to the monitoring port.
   * 
   * @param useSS
   * @param connections
   */
  public void collectConnections(boolean useSS,
      Map<String, Connection> connections) {
    collectConnections(useSS, connections, null);
  }

  /**
   * @return the monitoringPort
   */
  public int getMonitoringPort() {
    return monitoringPort;
  }

  public Map<String, Connection> readEstablishedConsWithSS(
      Map<String, Set<String>> pidInodes) {
    Map<String, Connection> connections = new HashMap<String, Connection>();
    try (BufferedReader reader = executeCommand(ssCommand)) {
      String line;
      while ((line = reader.readLine()) != null) {
    	Matcher m = SS_ESTABLISHED_LINE_FORMAT.matcher(line);
        if (m.matches()) {
          //LOG.debug("Matched: " + line);
          final String localIp = getIPv4Address(m.group(1));
          final String remoteIp = getIPv4Address(m.group(3));

          Connection connection =
              new Connection(localIp, Integer.parseInt(m.group(2)), remoteIp,
                  Integer.parseInt(m.group(4)));

          final String inode = m.group(9);
          connections.put(inode, connection);

          if (pidInodes != null) {
            try {
              String pidStr = m.group(7);
              if (Integer.parseInt(pidStr) > 0) {
                if (!pidInodes.containsKey(pidStr)) {
                  pidInodes.put(pidStr, new HashSet<String>());
                }
                pidInodes.get(pidStr).add(inode);
              }
            } catch (NumberFormatException e) {
              ;
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e, e);
    }

    return connections;
  }

  private static String getIPv4Address(String ipAddressStr) {
    String ip = ipAddressStr;
    if (ipAddressStr.contains(":")) {
      ip = checkAndConvertIPv6to4Address(ipAddressStr);
    }

    return ip;
  }

  private BufferedReader executeCommand(String cmd) throws IOException,
      InterruptedException {
    final String[] cmdargs = new String[] {"/bin/sh", "-c", cmd};
    ProcessBuilder builder = new ProcessBuilder(cmdargs);
    builder.redirectErrorStream(true);
    Process process = builder.start();
    process.waitFor();
    return new BufferedReader(new InputStreamReader(process.getInputStream(),
        StandardCharsets.UTF_8));
  }

  public Map<String, Connection> readProcNetTCP(String procfsDir) {
    Map<String, Connection> connections = new HashMap<String, Connection>();
    connections.putAll(parseProcNetTCP(Paths.get(procfsDir, "net", "tcp"),
        isDataNode, monitoringPort));

    Path path = Paths.get(procfsDir, "net", "tcp6");
    if (Files.exists(path)) {
      connections.putAll(parseProcNetTCP(path, isDataNode, monitoringPort));
    }

    return connections;
  }

  /**
   * Collect connections from or to a given monitoring port.
   * 
   * @param path
   *          file to read
   * @param isDatanode
   *          indicates whether it is a DataNode or a NodeManager
   * @param portToCheck
   *          the monitoring port
   * @return the map of connections and the related inode
   */
  public Map<String, Connection> parseProcNetTCP(Path path, boolean isDatanode,
      int portToCheck) {
    String line;
    Map<String, Connection> connections = new HashMap<String, Connection>();
    try (BufferedReader reader =
        Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      while ((line = reader.readLine()) != null) {
        Object[] objs = parseProcNetTCPLine(line, portToCheck, isDatanode);
        if (objs != null) {
          connections.put((String) objs[0], (Connection) objs[1]);
        }
      }
    } catch (IOException e) {
      LOG.error(e, e);
    }
    return connections;
  }

  private static Object[] parseProcNetTCPLine(String line, int portToCheck,
      boolean isDatanode) {
    Matcher m = PROC_NET_TCP_LINE_FORMAT.matcher(line);

    if (m.matches()) {
      if (!getState(m.group(5)).equals("ESTABLISHED")) {
        return null;
      }
      int localPort = getPortFromHex(m.group(2));
      int remotePort = getPortFromHex(m.group(4));
      //if (isDatanode
      //    && localPort != portToCheck) {
      ///  return null;
      //} else if (!isDatanode
      //    && remotePort != portToCheck) {
      //  return null;
      //}
      
      if(localPort != portToCheck && remotePort != portToCheck) {
    	  return null;
      }

      final String inode = m.group(13);
      String localIP = getIPv4AddressFromHex(m.group(1));
      String remoteIP = getIPv4AddressFromHex(m.group(3));

      return new Object[] {inode,
          new Connection(localIP, localPort, remoteIP, remotePort)};
    }

    return null;
  }

  private static String getIPv4AddressFromHex(String hexIpRecord) {
    String ip = null;
    if (hexIpRecord.length() == 8) {
      ip = toIPv4AddrString(hexIpRecord);
    } else {
      ip = checkAndConvertIPv6to4Address(toIPv6AddrString(hexIpRecord));
    }
    return ip;
  }

  /**
   * Convert a record of IP (has length of 8 bytes) in /proc/net/tcp.
   * 
   * @param hexStr
   *          record of IPv4 in the hex format
   * @return the valid representation of IPv4
   */
  public static String toIPv4AddrString(String hexStr) {
    long ipa = Long.parseLong(hexStr, 16);
    StringBuilder b = new StringBuilder();
    b.append(Long.toString(0x000000ff & (ipa)));
    b.append(".");
    b.append(Long.toString(0x000000ff & (ipa >> 8)));
    b.append(".");
    b.append(Long.toString(0x000000ff & (ipa >> 16)));
    b.append(".");
    b.append(Long.toString(0x000000ff & (ipa >> 24)));
    return b.toString();
  }

  private static int getPortFromHex(String hexPort) {
    return hex2Int(hexPort);
  }

  private static String getState(String hexPort) {
    int index = hex2Int(hexPort);
    if (index >= STATES.length) {
      index = 0;
    }
    return STATES[index];
  }

  private static int hex2Int(String hexStr) {
    return Integer.parseInt(hexStr, 16);
  }

  /**
   * Convert the record of IPv6 (has length of 32 bytes) in /proc/net/tcp6 to
   * the form of the IPv6 address.
   * 
   * @param rawNetTcpIpStr
   *          the record of IPv6 in the hex format
   * @return the valid representation of IPv6
   */
  public static String toIPv6AddrString(String rawNetTcpIpStr) {
    StringBuilder result = new StringBuilder();
    final char[] hexChars = rawNetTcpIpStr.trim().toCharArray();
    for (int i = 0; i < 4; i++) {
      for (int j = 3; j >= 0; j--) {
        int index = (8 * i + 2 * j);
        result.append(hexChars[index]).append(hexChars[index + 1]);
        result.append((j == 2) ? ":" : "");
      }
      result.append((i < 3) ? ":" : "");
    }
    return result.toString();
  }

  /**
   * Convert an IPv6 into the IPv4 format.
   * 
   * @param ipv6AddrStr
   *          the IPv6 record
   * @return an IPv4 address
   */
  public static String checkAndConvertIPv6to4Address(String ipv6AddrStr) {
    InetAddress inetAddr = InetAddresses.forString(ipv6AddrStr);
    if (InetAddresses.isMappedIPv4Address(ipv6AddrStr)) {
      return InetAddresses.toAddrString(inetAddr);
    } else if (inetAddr instanceof Inet6Address) {
      Inet6Address inet6Addr = (Inet6Address) inetAddr;
      if (InetAddresses.hasEmbeddedIPv4ClientAddress(inet6Addr)) {
        return InetAddresses.getEmbeddedIPv4ClientAddress(inet6Addr)
            .getHostAddress();
      }

      return InetAddresses.toAddrString(inetAddr);
    }

    return null;
  }
}
