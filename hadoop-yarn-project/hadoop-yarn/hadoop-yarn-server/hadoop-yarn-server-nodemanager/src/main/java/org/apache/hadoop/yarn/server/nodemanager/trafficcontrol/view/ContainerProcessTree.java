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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.impl.FactoryHelper;

/**
 * This class contains process data (process id, child processes and inodes) of
 * a NM container.
 *
 */
public class ContainerProcessTree {

  private static final Log LOG = LogFactory.getLog(ContainerProcessTree.class);

  private String containerId;

  private CommonProcessTree processTree;

  private Map<String, CommonProcessTree> containerPIDTree = new HashMap<>();

  private Set<String> inodes = new HashSet<String>();

  public ContainerProcessTree(String containerId, CommonProcessTree tree) {
    this.processTree = tree;
    this.containerId = containerId;
  }

  /**
   * @return the processTree
   */
  public CommonProcessTree getProcessTree() {
    return processTree;
  }

  public void buildPIDTree(boolean excludeRootPID) {
    containerPIDTree.clear();
    inodes.clear();

    LinkedList<CommonProcessTree> allChildrenQueue = new LinkedList<>();
    allChildrenQueue.add(processTree);

    while (!allChildrenQueue.isEmpty()) {
      CommonProcessTree child = allChildrenQueue.remove();
      String childPid = child.getPid();

      if (!containerPIDTree.containsKey(childPid)) {
        containerPIDTree.put(childPid, child);
      }
      allChildrenQueue.addAll(child.getChildren());
    }
  }

  public Set<String> getCurrentPIDList() {
    return containerPIDTree.keySet();
  }

  /**
   * Collect inodes of this process tree. Note that only /proc/PID/fd can only
   * walked by the process' owner.
   */
  public void collectInodes() {
    inodes.clear();
    for (String pid : containerPIDTree.keySet()) {
      inodes.addAll(collectInodesOfPID(pid));
    }
  }

  /**
   * Collect all socket inodes of the given PID.
   * 
   * @param pid
   *          the porocess id
   * @return the list of socket inodes of the given pid
   */
  public Set<String> collectInodesOfPID(String pid) {
    Set<String> inodes = new HashSet<String>();
    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get(FactoryHelper.getInstance()
            .getProcFsRoot(), pid, "fd"))) {
      for (Path link : directoryStream) {
        // if (Files.isSymbolicLink(link)) {
        String inode = Files.readSymbolicLink(link).toString();
        // socket:[21831257]
        if (inode.startsWith("socket")) {
          inodes.add(inode.replace("socket:[", "").replace("]", ""));
        }
        // }
      }
    } catch (
        NoSuchFileException | AccessDeniedException e) {
      ;// When process is complete
    } catch (IOException ex) {
      LOG.error("Cannot read inoded of process "
          + pid, ex);
    }

    return inodes;
  }

  /**
   * @return the inodes
   */
  public Set<String> getInodes() {
    return inodes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ContainerProcessTree [containerId="
        + containerId + ", pTree=" + processTree + ", childPids="
        + containerPIDTree.keySet() + "]";
  }
}
