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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.AbstractYarnContainerReportService;

/**
 * In the client mode, Yarn can use this plug-in to cooperate with
 * {@link FileBasedContainerService}.
 *
 */
public class FileBasedYarnContainerReporter
    extends AbstractYarnContainerReportService {

  static final Log LOG =
      LogFactory.getLog(FileBasedYarnContainerReporter.class);

  private Path storagePath;

  public FileBasedYarnContainerReporter(Configuration conf) {
    super(conf);
    storagePath = FileBasedContainerService.getStorageDir(conf);
  }

  @Override
  public boolean addMonitoringContainer(String containerId, float rateInMbps) {
    String content = String.format("rate=%.2f%n", rateInMbps);
    Path containerFile = storagePath.resolve(containerId);
    try {
      Files.write(containerFile, content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Cannot create file "
          + containerFile + " with line: " + content, e);
      return false;
    }
    return true;
  }

  @Override
  public boolean registerPid(String containerId, int pid) {
    Path containerFile = storagePath.resolve(containerId);
    if (Files.exists(containerFile)) {
      try {
        List<String> lines =
            Files.readAllLines(containerFile, StandardCharsets.UTF_8);
        boolean foundRate = false;
        if (lines != null) {
          StringBuffer content = new StringBuffer();
          for (String line : lines) {
            if (line.startsWith("rate=")) {
              foundRate = true;
            }
            if (!line.startsWith("pid=")) {
              content.append(line).append("\n");
            }
          }

          if (foundRate) {
            content.append("pid=").append(pid).append("\n");
            Files.write(containerFile,
                content.toString().getBytes(StandardCharsets.UTF_8));
            return true;
          }
        }

        LOG.warn("We shoudn't reach here: No rate is specifed for container: "
            + containerId + " in " + containerFile);

      } catch (IOException e) {
        LOG.error("Error occured when processing file "
            + containerFile, e);
      }
    } else {
      LOG.error("File "
          + containerFile + " is not existed. Nothing to do!");
    }
    return false;
  }

  @Override
  public boolean updateContainerRate(String containerId, float rateInMbps) {
    Path containerFile = storagePath.resolve(containerId);
    if (Files.exists(containerFile)) {
      try {
        List<String> lines =
            Files.readAllLines(containerFile, StandardCharsets.UTF_8);
        StringBuffer content = new StringBuffer();
        if (lines != null) {
          for (String line : lines) {
            if (!line.startsWith("rate=")) {
              content.append(line).append("\n");
            }
          }
        }

        content.append("rate=").append(rateInMbps).append("\n");
        Files.write(containerFile,
            content.toString().getBytes(StandardCharsets.UTF_8));
        return true;

      } catch (IOException e) {
        LOG.error("Error occured when processing file "
            + containerFile, e);
      }
    } else {
      LOG.error("File "
          + containerFile + " is not existed. Nothing to do!");
    }
    return false;
  }

  @Override
  public void stopMonitoringContainer(String containerId) {
    Path containerFile = storagePath.resolve(containerId);
    try {
      Files.deleteIfExists(containerFile);
    } catch (IOException e) {
      LOG.error("Cannot delete file "
          + containerFile, e);
    }

  }

  @Override
  public void initialize(String localNodeId) {
    if (!Files.exists(storagePath)) {
      try {
        Files.createDirectory(storagePath);
      } catch (IOException e) {
        ;
      }
    }
  }

  @Override
  public void start() {
    return;
  }

  @Override
  public void stop() {
    return;
  }
}
