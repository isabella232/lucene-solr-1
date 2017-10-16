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

import java.nio.file.Path;

import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;

public class Solr4Runner extends AbstractRunner {
  private final DockerCommandExecutor executor;
  private Path coresDir = createTempDirQuick("cores.dir");

  Solr4Runner(DockerRunner.Context context) {
    super(context);
    final ContainerCreation creation;
    try {
      HostConfig.Builder hostConfig = HostConfig.builder()
          .portBindings(DockerRunner.SOLR_PORT_BINDINGS);
      hostConfig.appendBinds(HostConfig.Bind.from(coresDir.toString()).to(DockerRunner.CORES_SUB_DIR).build());
      ContainerConfig containerConfig = getSolrContainerConfig(hostConfig);

      creation = newContainer(containerConfig);
      final String id = creation.id();
      docker.startContainer(id);

      executor = new DockerCommandExecutor(docker, id);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  public void start() {
    startSolr4(executor);
  }

  public void stop() {
    stopSolr4(executor);
  }

  public String getNodeDir() {
    return coresDir.toString();
  }
}
