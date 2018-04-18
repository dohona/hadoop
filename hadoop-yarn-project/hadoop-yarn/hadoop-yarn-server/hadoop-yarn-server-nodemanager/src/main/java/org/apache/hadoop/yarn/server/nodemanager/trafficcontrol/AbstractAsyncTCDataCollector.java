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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Collect TC settings from back-end storages. In this case TrafficControl will
 * not start the monitoring thread to collect the update from the back-end
 * storage.
 * 
 */
public abstract class AbstractAsyncTCDataCollector extends
    AbstractTCDataCollector {

  private static final Log LOG = LogFactory
      .getLog(AbstractAsyncTCDataCollector.class);

  private AbstractTrafficController tcController;

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  private UpdateRequest updateRequest = null;

  public AbstractAsyncTCDataCollector(Configuration conf) {
    super(conf);
  }

  /**
   * Register TrafficController as callback.
   * 
   * @param callBack
   */
  public final void registerCallback(AbstractTrafficController callBack) {
    this.tcController = callBack;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.yarn.server.nodemanager.trafficcontrol.
   * AbstractTCDataCollector#stop()
   */
  @Override
  public void stop() {
    try {
      executor.shutdownNow();
    } catch (Exception e) {
      ;
    }
    super.stop();
  }

  /**
   * This function should be called when update is found. Only one update can be
   * in progress, and one can be queued.
   */
  public final synchronized void notifyUpdate() {
    if (!isReady()) {
      return;
    }

    LOG.debug("notifyUpdate()");
    synchronized (AbstractAsyncTCDataCollector.this) {
      if (updateRequest == null) {
        updateRequest = new UpdateRequest(new Runnable() {
          @Override
          public void run() {
            try {
              LOG.debug("Calling update()...");
              tcController.update();
            } catch (RuntimeException e) {
              LOG.error("notifyUpdate error: "
                  + e.getMessage(), e);
            }
          }
        });
      }

      if (!updateRequest.inProgress) {
        updateRequest.submit();
      } else {
        updateRequest.queued = true;
      }
    }
  }

  private final class UpdateRequest implements Runnable {
    private final Runnable item;
    private boolean queued;
    private boolean inProgress;

    private UpdateRequest(Runnable item) {
      this.item = item;
      this.queued = true;
      this.inProgress = false;
    }

    // Caller must have a lock
    private void submit() {
      LOG.debug("Submit update");
      executor.submit(this);
    }

    public void run() {
      try {
        synchronized (AbstractAsyncTCDataCollector.this) {
          assert queued;
          queued = false;
          inProgress = true;
        }
        item.run();
      } finally {
        synchronized (AbstractAsyncTCDataCollector.this) {
          inProgress = false;
          if (queued) {
            // another submit for this job is requested while we
            // were doing the update. Do it again.
            submit();
          }
        }
      }
    }
  }
}
