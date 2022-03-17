/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import java.io.*;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStreamer {
  private BufferedReader deploymentStreamFileReader;
  private FileInputStream deploymentFileStream;
  private Logger logger = LoggerFactory.getLogger(DeployController.class);

  enum LogStreamerState {
    STATE_NOT_STARTED("not started"),
    STATE_PAUSED("paused"),
    STATE_RUNNING("running");
    private String state;

    private LogStreamerState(String state) {
      this.state = state;
    }
  };

  private LogStreamerState state = LogStreamerState.STATE_NOT_STARTED;

  public LogStreamerState getState() {
    return state;
  }

  public void startFresh() {
    try {
      // ensure reset
      reset();
      // ensure file is created & erased, or truncated to 0 if it existed before
      new PrintWriter(Constants.DEPLOYMENT_STREAMING_LOG_FILE).close();
      deploymentFileStream = new FileInputStream(Constants.DEPLOYMENT_STREAMING_LOG_FILE);
      deploymentStreamFileReader = new BufferedReader(new InputStreamReader(deploymentFileStream));
      state = LogStreamerState.STATE_RUNNING;
    } catch (IOException e) {
      logger.error("An exception happened during start: " + e.getMessage());
    }
  }

  public void pause() {
    state = LogStreamerState.STATE_PAUSED;
  }

  private void resume() {
    if (state != LogStreamerState.STATE_NOT_STARTED) {
      state = LogStreamerState.STATE_RUNNING;
    }
  }

  public void reset() {
    if (state == LogStreamerState.STATE_NOT_STARTED) return;
    try {
      deploymentFileStream.close();
      deploymentStreamFileReader.close();
    } catch (final Exception e) {
      logger.error("An exception happened during reset: " + e.getMessage());
    } finally {
      state = LogStreamerState.STATE_NOT_STARTED;
    }
  }

  public ArrayList<String> getStreamingLogs(boolean reset) {
    String s = null;
    ArrayList<String> messages = new ArrayList<String>();
    if (reset) {
      resume();
    }
    if (state == LogStreamerState.STATE_PAUSED || state == LogStreamerState.STATE_NOT_STARTED) {
      return messages;
    }
    try {
      if (reset) {
        // seek to beginning of the file
        deploymentFileStream.getChannel().position(0);
        deploymentStreamFileReader =
            new BufferedReader(new InputStreamReader(deploymentFileStream));
      }
      while (deploymentStreamFileReader.ready()
          && (s = deploymentStreamFileReader.readLine()) != null) {
        messages.add(s);
      }
    } catch (final Exception e) {
      logger.error("An exception happened during reading: " + e.getMessage());
    }
    return messages;
  }
}
