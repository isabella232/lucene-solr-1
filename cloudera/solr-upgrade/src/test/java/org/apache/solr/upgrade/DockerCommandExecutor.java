/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.upgrade;

import java.nio.charset.StandardCharsets;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.ExecState;
import org.apache.solr.common.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.spotify.docker.client.DockerClient.ExecCreateParam.attachStderr;
import static com.spotify.docker.client.DockerClient.ExecCreateParam.attachStdout;
import static com.spotify.docker.client.DockerClient.ExecCreateParam.detach;

/**
 * Execute a command in an already existing docker container
 */
public class DockerCommandExecutor {
  private static Logger log = LoggerFactory.getLogger(DockerCommandExecutor.class);
  private DockerClient docker;
  private String containerId;

  public DockerCommandExecutor(DockerClient docker, String container) {
    this.docker = docker;
    this.containerId = container;
  }

  public class ExecutionResult {
    private StringBuilder allLines;

    public ExecutionResult(StringBuilder allLines) {
      this.allLines = allLines;
    }

    public String getStdout() {
      return allLines.toString();
    }
  }

  public ExecutionResult execute(String... cmd) {
    log.info("Executing command: {}", String.join(" ", cmd));
    try {
      return executeCommand(containerId, cmd);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void executeDetached(String... cmd) {
    log.info("Executing command: {}", String.join(" ", cmd));
    try {
      executeCommandDetached(containerId, cmd);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ExecutionResult executeCommand(String id, String... command) throws DockerException, InterruptedException {
    ExecCreation pwd = docker.execCreate(id, command, attachStdout(), attachStderr());
    LogStream pwdLog = docker.execStart(pwd.id());
    return waitCompletion(pwd, pwdLog);
  }

  public ExecutionResult waitCompletion(ExecCreation execCreation, LogStream pwdLog) throws DockerException, InterruptedException {
    ExecState pwdRes;
    StringBuilder allLines = new StringBuilder();
    while (pwdLog.hasNext() || (pwdRes = docker.execInspect(execCreation.id())).running()) {
      if(pwdLog.hasNext()) {
        String line = nextLine(pwdLog);
        allLines.append(line).append('\n');
        log.info("docker container: " + line);
      }
      Thread.sleep(100);
    }
    if(pwdRes.exitCode() != 0 )
      throw new DockerCommandExecutionException(allLines.toString(), pwdRes.exitCode());
    return new ExecutionResult(allLines);
  }

  private void executeCommandDetached(String id, String... command) throws DockerException, InterruptedException {
    StringBuilder allLines = new StringBuilder();
    ExecCreation pwd = docker.execCreate(id, command, detach());
    docker.execStart(pwd.id());
  }

  private String nextLine(LogStream startSolrLogs) {
    LogMessage log = startSolrLogs.next();
    return StandardCharsets.UTF_8.decode(log.content()).toString();
  }


}
