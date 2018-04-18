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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Strings;

/**
 * File based plug-in of the container service. It monitoring a given local
 * folder for new containers.
 *
 */
public class FileBasedContainerService extends AbstractContainerService {

  static final Log LOG = LogFactory.getLog(FileBasedContainerService.class);

  private static final String WATCHER_DIR_NAME = "monitoring_containers";

  private Path monitoringDir;
  private MonitorThread monitorThread;

  private boolean initialized = false;
  private volatile boolean done = false;
  private boolean isEnabled = false;

  private Map<String, Long> modifiedTimes = new HashMap<String, Long>();

  public FileBasedContainerService(Configuration conf) {
    super(conf);
    monitoringDir = getStorageDir(conf);
  }

  public static Path getStorageDir(Configuration conf) {
    Path storageDir = null;
    final String localPath =
        conf.get(YarnConfiguration.NM_HDFS_BE_CONTAINER_DATA_LOCAL_PATH, null);
    if (!Strings.isNullOrEmpty(localPath)) {
      storageDir = Paths.get(localPath.trim());
    } else {
      storageDir =
          Paths.get(System.getProperty("java.io.tmpdir"), WATCHER_DIR_NAME);
    }

    return storageDir;
  }

  @Override
  protected float normalize(float rate) {
    return rate;
  }

  @Override
  public void initialize(String localNodeId) {
    try {
      LOG.info("FileBasedContainerService: Folder to check containers is "
          + monitoringDir);
      if (!Files.exists(monitoringDir)) {
        Files.createDirectory(monitoringDir);
      }

      monitorThread = new MonitorThread();
      isEnabled = true;

    } catch (IOException e) {
      LOG.error("Cannot create watch service: "
          + e.getMessage(), e);
    }
  }

  private void apply(Path containerFile, String clsId) throws IOException {
    if (!Files.isRegularFile(containerFile)) {
      return;
    }

    long lastModifiedTime = 0;
    if (modifiedTimes.containsKey(clsId)) {
      lastModifiedTime = modifiedTimes.get(clsId);
    }

    if (Files.getLastModifiedTime(containerFile).toMillis()
        - lastModifiedTime < 1000) {
      return;
    }

    modifiedTimes.put(clsId, Files.getLastModifiedTime(containerFile)
        .toMillis());

    float rate = 0;
    int pid = 0;
    try {
      List<String> lines =
          Files.readAllLines(containerFile, StandardCharsets.UTF_8);
      if (lines != null) {
        for (String line : lines) {
          String trimmedLine = line.replaceAll("\\s", "");
          if (trimmedLine.startsWith("rate=")) {
            rate = getRate(trimmedLine.substring(5));
          }

          if (trimmedLine.startsWith("pid=")) {
            pid = getPid(trimmedLine.substring(4));
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e);
    }

    if (rate > 0
        && pid >= 0) {

      if (initialized
          || pid > 1) {
        addMonitoringContainer(clsId, rate);
      }
      if (pid > 1) {
        registerPid(clsId, pid);
      }
    }
  }

  private int getPid(String pid) {
    try {
      return Integer.parseInt(pid);
    } catch (
        NumberFormatException | NullPointerException e) {
      return -1;
    }
  }

  private float getRate(String rate) {
    try {
      return Float.parseFloat(rate);
    } catch (
        NumberFormatException | NullPointerException e) {
      return -1;
    }
  }

  @Override
  public void start() {
    if (!isEnabled) {
      return;
    }
    LOG.info("Starting monitorThread");
    monitorThread.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping monitorThread");
    done = true;
  }

  private void init() {
    LOG.info("Walking the directory: "
        + monitoringDir);
    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(monitoringDir)) {
      for (Path containerFile : directoryStream) {
        String clsId = containerFile.getFileName().toString();
        Path fullChildPath = monitoringDir.resolve(containerFile);
        if (!Files.isDirectory(fullChildPath)) {
          LOG.info("Found existing file: "
              + fullChildPath);
          apply(fullChildPath, clsId);
        }
      }
    } catch (IOException e) {
      LOG.warn("Error occurred when reading "
          + monitoringDir, e);
    }

    initialized = true;
  }

  @SuppressWarnings("unchecked")
  private void process() throws IOException {
    // we register three events. i.e. whenever a file is created, deleted or
    // modified the watcher gets informed
    try (WatchService ws = FileSystems.getDefault().newWatchService();) {
      WatchKey key =
          monitoringDir.register(ws, StandardWatchEventKinds.ENTRY_CREATE,
              StandardWatchEventKinds.ENTRY_DELETE,
              StandardWatchEventKinds.ENTRY_MODIFY);

      // we can poll for events in an infinite loop
      while (!done) {
        try {
          // the take method waits till watch service receives a
          // notification
          key = ws.take();
        } catch (InterruptedException e) {
          ;
        }

        // once a key is obtained, we poll for events on that key
        modifiedTimes.clear();
        List<WatchEvent<?>> keys = key.pollEvents();

        for (WatchEvent<?> watchEvent : keys) {
          WatchEvent<Path> ev = (WatchEvent<Path>) watchEvent;
          Path filename = ev.context();
          Path fullChildPath = monitoringDir.resolve(filename);
          if (Files.isDirectory(fullChildPath)) {
            continue;
          }

          final String containerId = filename.toString();
          Kind<?> eventType = watchEvent.kind();
          if (eventType == StandardWatchEventKinds.OVERFLOW) {
            continue;
          }

          try {
            if (eventType == StandardWatchEventKinds.ENTRY_CREATE) {
              LOG.info("New container detected: "
                  + containerId);
              apply(fullChildPath, containerId);
            } else if (eventType == StandardWatchEventKinds.ENTRY_MODIFY) {
              LOG.info("Changes detected for container "
                  + containerId);
              apply(fullChildPath, containerId);
            } else if (eventType == StandardWatchEventKinds.ENTRY_DELETE) {
              LOG.info("Container is stopped: "
                  + containerId);
              stopMonitoringContainer(containerId);
            }
          } catch (Exception e) {
            continue;
          }
        }

        // we need to reset the key so the further key events may be
        // polled
        if (!key.reset()) {
          break;
        }
      }
    }
    // close the watcher service
    // watchService.close();
  }

  class MonitorThread extends Thread {

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {

      init();
      while (!done) {
        try {
          process();
        } catch (IOException e) {
          LOG.error("Error occured: "
              + e.getMessage(), e);
        }

        if (!done
            && !Files.exists(monitoringDir)) {
          LOG.warn(monitoringDir
              + " is deleted. Recreate it and continue.");
          try {
            Files.createDirectory(monitoringDir);
          } catch (IOException e1) {
            LOG.error("Error occured: "
                + e1.getMessage(), e1);
            break;
          }
        }
      }
    }
  }
}
