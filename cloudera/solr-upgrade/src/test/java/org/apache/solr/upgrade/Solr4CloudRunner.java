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
import java.nio.file.Path;
import java.nio.file.Paths;

import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import org.apache.commons.io.FileUtils;

/**
 * Start/stop CDH5 Solr in cloud mode in a docker container (linked with a ZooKeeper)
 */
public class Solr4CloudRunner extends AbstractRunner {
  private final DockerCommandExecutor executor;
  private final String zkId;
  private final String nodeSubDir;
  private final String nodeDir;
  private static int solrNodeCounter = 0;
  private Path coresDir = createTempDirQuick("cores.dir");
  private Path workDir = createTempDirQuick("work-temp-dir");
  private final String containerId;


  public Solr4CloudRunner(ZooKeeperRunner zooKeeper, DockerRunner.Context dockerContext) {
    super(dockerContext);
    nodeSubDir = "node" + solrNodeCounter++ + "/solr";
    nodeDir = Paths.get(coresDir.toString(), nodeSubDir).toString();
    ContainerCreation creation;
    try {

      zkId = zooKeeper.getContainerId();
      HostConfig.Builder hostConfig = HostConfig.builder()
          .portBindings(DockerRunner.SOLR_PORT_BINDINGS)
          .appendBinds(HostConfig.Bind.from(workDir.toString()).to(WORK_DIR).build())
          .links(zkId);
      hostConfig.appendBinds(HostConfig.Bind.from(coresDir.toString()).to(DockerRunner.CORES_SUB_DIR).build());
      ContainerConfig containerConfig = getSolrContainerConfig(hostConfig);

      creation = newContainer(containerConfig);
      containerId = creation.id();
      docker.startContainer(containerId);

      executor = new DockerCommandExecutor(docker, containerId);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  public void start() {
    SolrArgsBuilder baseStartArgs = new SolrArgsBuilder(DockerRunner.SOLR_FROM).start().port(DockerRunner.SOLR_PORT)
        .cloudMode(zkId + ":" + DockerRunner.ZK_PORT + "/solr")
        .home(DockerRunner.CORES_SUB_DIR + "/" + nodeSubDir);
    startSolr(executor, baseStartArgs, DockerRunner.SOLR_FROM + "/example/logs/solr.log");
  }

  public void startWithHdfs() {
    SolrArgsBuilder baseStartArgs = new SolrArgsBuilder(DockerRunner.SOLR_FROM).start().port(DockerRunner.SOLR_PORT)
        .cloudMode(zkId+":"+ DockerRunner.ZK_PORT + "/solr")
        .withHdfs("nn:50070", DockerRunner.CORES_SUB_DIR + "/" + nodeSubDir);
    startSolr(executor, baseStartArgs, DockerRunner.SOLR_FROM + "/example/logs/solr.log");
  }

  public void stop() {
    DockerCommandExecutor.ExecutionResult stop = executor.execute(new SolrArgsBuilder(DockerRunner.SOLR_FROM).stop()
        .cloudMode(zkId + ":" + DockerRunner.ZK_PORT + "/solr")
        .allInstances()
        .build());
    killContainer();
  }

  private void killContainer() {
    try {
      docker.killContainer(containerId);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public String getNodeDir() {
    return nodeDir;
  }

  public Path getSchemaCopy(String collectionName) {
    executor.execute("wget", "-O", WORK_DIR + "/schema.xml", "localhost:8983/solr/solrj_collection/schema?wt=schema.xml");
    return workDir.resolve("schema.xml");
  }

  public Path getIndexCopy() {
    try {
      Path original = Paths.get(nodeDir, "solrj_collection_shard1_replica1/data/index");
      Path res = createTempDir("index");
      FileUtils.copyDirectoryToDirectory(original.toFile(), res.toFile());
      return res;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
