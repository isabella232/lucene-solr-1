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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;

import static org.apache.solr.upgrade.DockerRunner.SOLR_LOG_LOCATION;
import static org.apache.solr.upgrade.DockerRunner.SOLR_TO;

/**
 * Start/stop Solr in cloud mode in a docker container (given a ZooKeeper)
 */
public class SolrCloudRunner extends AbstractRunner implements SolrRunner {
  private final DockerCommandExecutor executor;
  private final String zkId;
  private final String nodeSubDir;
  private final String nodeDir;
  private int solrNodeCounter = 0;
  private Path coresDir = createTempDirQuick("cores.dir");

  SolrCloudRunner(ZooKeeperRunner zooKeeper, DockerRunner.Context context) {
    super(context);
    nodeSubDir = "node" + solrNodeCounter++ + "/solr";
    nodeDir = Paths.get(coresDir.toString(), nodeSubDir).toString();
    try {
      Files.createDirectories(Paths.get(nodeDir));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      zkId = zooKeeper.getContainerId();
      HostConfig.Builder hostConfig = HostConfig.builder()
          .portBindings(DockerRunner.SOLR_PORT_BINDINGS)
          .links(zkId);
      hostConfig.appendBinds(HostConfig.Bind.from(coresDir.toString()).to(DockerRunner.CORES_SUB_DIR).build());
      ContainerConfig containerConfig = getSolrContainerConfig(hostConfig);

      ContainerCreation creation = newContainer(containerConfig);
      final String id = creation.id();
      docker.startContainer(id);

      executor = new DockerCommandExecutor(docker, id);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  public void start() {
    SolrArgsBuilder baseStartArgs = new SolrArgsBuilder(DockerRunner.SOLR_TO).start().port(DockerRunner.SOLR_PORT)
        .cloudMode(zkId + ":" + DockerRunner.ZK_PORT + "/solr")
        .home(DockerRunner.CORES_SUB_DIR + "/" + nodeSubDir)
        .force();
    startSolr(executor, baseStartArgs, DockerRunner.SOLR_TO + "/server/logs/solr.log");
  }

  public void stop() {
    stopSolr4(executor);
  }

  @Override
  public void dumpLogFileIfPossible() {
    dumpSolrLogIfPossible(executor, SOLR_TO + SOLR_LOG_LOCATION);
  }


  public String getNodeDir() {
    return nodeDir;
  }
}
