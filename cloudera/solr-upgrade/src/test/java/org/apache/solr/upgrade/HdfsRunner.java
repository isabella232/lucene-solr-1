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

import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.docker.client.messages.Volume;

public class HdfsRunner extends AbstractRunner {
  private String nameNodeContainerId;
  private String dataNodeContainerId;

  public HdfsRunner(DockerRunner.Context context) {
    super(context);
  }

  public DockerCommandExecutor nameNodeExecutor() {
    return new DockerCommandExecutor(docker, nameNodeContainerId);
  }

  public DockerCommandExecutor dataNodeExecutor() {
    return new DockerCommandExecutor(docker, dataNodeContainerId);
  }


  public void stop() {
    try {
      docker.stopContainer(dataNodeContainerId, 5);
      docker.stopContainer(nameNodeContainerId, 5);
    } catch (DockerException | InterruptedException e) {
      e.printStackTrace();
    }
  }


  public void start() {
    try {
      Volume dataVolume = Volume.builder().driver("local").name("hdfs-data").mountpoint("/hadoop/dfs/data").build();
      Volume nameVolume = Volume.builder().driver("local").name("hdfs-name").mountpoint("/hadoop/dfs/name").build();
      docker.createVolume(dataVolume);
      docker.createVolume(nameVolume);

      HostConfig.Builder nnHostConfig = HostConfig.builder().portBindings(ImmutableMap.of(
          DockerRunner.HDFS_NAMENODE_PORT, Arrays.asList(PortBinding.of("0.0.0.0", DockerRunner.HDFS_NAMENODE_PORT))
      ));

      ContainerConfig containerConfig = baseContainerConfig(nnHostConfig.build())
          .image(DockerRunner.NAMENODE_IMAGE_NAME)
          .exposedPorts(DockerRunner.HDFS_NAMENODE_PORT)
          .labels(ImmutableMap.of(DockerRunner.DOCKER_LABEL, ""))
          .hostname("nn")
          .addVolume("hdfs-name:/hadoop/dfs/name")
          .exposedPorts(DockerRunner.HDFS_NAMENODE_PORT)
          .env("CLUSTER_NAME=solr")
          .build();

      String nameNodeContainerName = namePrefix + "-NN-" + containerCounter++;
      final ContainerCreation creation = docker.createContainer(containerConfig, nameNodeContainerName);

      this.nameNodeContainerId = creation.id();
      docker.startContainer(nameNodeContainerId);

      HostConfig.Builder dnHostConfig = HostConfig.builder();

      ContainerConfig dnContainerConfig = baseContainerConfig(dnHostConfig.build())
          .image(DockerRunner.DATANODE_IMAGE_NAME)
          .labels(ImmutableMap.of(DockerRunner.DOCKER_LABEL, ""))
          .hostname("dn")
          .addVolume("hdfs-data:/hadoop/dfs/data")

//            .env("CORE_CONF_fs_defaultFS=hdfs://"+ nameNodeContainerName + ":8020")
          .env("CORE_CONF_fs_defaultFS=hdfs://nn:8020")
          .build();

      final ContainerCreation dnCreation = docker.createContainer(dnContainerConfig, namePrefix + "-DN-" + containerCounter++);

      this.dataNodeContainerId = dnCreation.id();
      docker.startContainer(dataNodeContainerId);

      DockerCommandExecutor nameNodeExecutor = new DockerCommandExecutor(docker, nameNodeContainerId);
      waitHdfsUp(nameNodeExecutor);
    } catch (DockerException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void waitHdfsUp(DockerCommandExecutor commandExecutor) {
    boolean started = false;
    long before = System.nanoTime();
    while (!started) {
      DockerCommandExecutor.ExecutionResult result = null;
      try {
        result = commandExecutor.execute("curl", "-s", "-H", "Accept: application/json", "http://localhost:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus");
        String stdout = result.getStdout();
        started = stdout.contains("\"State\" : \"active\"");
      } catch (Exception e) {
        LOG.debug("Error: {}", e.getMessage());
      }

      if (elapsedSeconds(before) > 15) {
        throw new RuntimeException("HDFS cannot be started");
      }
      sleep(500);
    }
  }


}
