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

/**
 * Start/stop HDFS minicluster
 */
public class HdfsRunner extends AbstractRunner {
  public static final int SECONDS_TO_WAIT_BEFORE_KILLING_HDFS = 5;
  public static final int HDFS_MAX_STARTUP_SECONDS = 15;
  public static final int HDFS_UP_POLL_DELAY_MILLIS = 500;

  public static final String CURL_SILENT_OPTION = "-s";
  public static final String CURL_HEADER_OPTION = "-H";
  public static final String ACCEPT_JSON_HTTP_HEADER = "Accept: application/json";
  public static final String QUERY_NAMENODE_STATUS_URL = "http://localhost:" + DockerRunner.HDFS_NAMENODE_PORT 
      + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";
  public static final String NAMENODE_JMX_STATUS_ACTIVE = "\"State\" : \"active\"";
  public static final String NAMENODE_HOST_NAME = "nn";
  public static final String DATANODE_HOST_NAME = "dn";
  public static final String NAMENODE_DEFAULT_PORT = "8020";
  public static final String HDFS_DATA_VOLUME = "hdfs-data";
  public static final String HDFS_NAME_VOLUME = "hdfs-name";
  public static final String HDFS_DATA_MOUNT = "/hadoop/dfs/data";
  public static final String HDFS_NAME_MOUNT = "/hadoop/dfs/name";

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
      docker.stopContainer(dataNodeContainerId, SECONDS_TO_WAIT_BEFORE_KILLING_HDFS);
      docker.stopContainer(nameNodeContainerId, SECONDS_TO_WAIT_BEFORE_KILLING_HDFS);
    } catch (DockerException | InterruptedException e) {
      LOG.info("Failed to kill container", e);
    }
  }


  public void start() {
    try {
      Volume dataVolume = Volume.builder().driver("local").name(HDFS_DATA_VOLUME).mountpoint(HDFS_DATA_MOUNT).build();
      Volume nameVolume = Volume.builder().driver("local").name(HDFS_NAME_VOLUME).mountpoint(HDFS_NAME_MOUNT).build();
      docker.createVolume(dataVolume);
      docker.createVolume(nameVolume);

      HostConfig.Builder nnHostConfig = HostConfig.builder().portBindings(ImmutableMap.of(
          DockerRunner.HDFS_NAMENODE_PORT, Arrays.asList(PortBinding.of("0.0.0.0", DockerRunner.HDFS_NAMENODE_PORT))
      ));

      ContainerConfig containerConfig = baseContainerConfig(nnHostConfig.build())
          .image(DockerRunner.NAMENODE_IMAGE_NAME)
          .exposedPorts(DockerRunner.HDFS_NAMENODE_PORT)
          .labels(ImmutableMap.of(DockerRunner.DOCKER_LABEL, ""))
          .hostname(NAMENODE_HOST_NAME)
          .addVolume(HDFS_NAME_VOLUME + ":" + HDFS_NAME_MOUNT)
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
          .hostname(DATANODE_HOST_NAME)
          .addVolume(HDFS_DATA_VOLUME + ":" + HDFS_DATA_MOUNT)
          .env("CORE_CONF_fs_defaultFS=hdfs://" + NAMENODE_HOST_NAME + ":" + NAMENODE_DEFAULT_PORT) // see: https://hub.docker.com/r/uhopper/hadoop/
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
        result = commandExecutor.execute("curl", CURL_SILENT_OPTION, CURL_HEADER_OPTION, ACCEPT_JSON_HTTP_HEADER, QUERY_NAMENODE_STATUS_URL);
        String stdout = result.getStdout();
        started = stdout.contains(NAMENODE_JMX_STATUS_ACTIVE);
      } catch (Exception e) {
        LOG.debug("Error: {}", e.getMessage());
      }

      if (elapsedSecondsSince(before) > HDFS_MAX_STARTUP_SECONDS) {
        throw new RuntimeException("HDFS cannot be started");
      }
      sleep(HDFS_UP_POLL_DELAY_MILLIS);
    }
  }


}
