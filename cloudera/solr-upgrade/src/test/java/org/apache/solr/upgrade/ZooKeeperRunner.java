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

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static com.spotify.docker.client.DockerClient.LogsParam.tail;
import static org.apache.solr.upgrade.DockerRunner.DOCKER_LABEL;
import static org.apache.solr.upgrade.DockerRunner.SOLR_TO;
import static org.apache.solr.upgrade.DockerRunner.ZK_DIR;
import static org.apache.solr.upgrade.DockerRunner.ZK_PORT;
import static org.apache.solr.upgrade.DockerRunner.ZK_PORT_BINDING;

/**
 * Runs ZooKeeper in a Docker image
 */
public class ZooKeeperRunner extends AbstractRunner {
  public static final int MAX_LOG_COUNT = 500;
  public static final int REQUIRED_SUCCESSFUL_CHECKS = 2;
  public static final int MAX_ZOOKEEPER_STARTUP_SECONDS = 15;
  public static final int ZOOKEEPER_PING_DELAY = 500;
  private String containerId;

  public ZooKeeperRunner(DockerRunner.Context context) {
    super(context);
  }

  public void start() {
    containerId = startZookeeperDetached();
  }

  public String getContainerId() {
    return containerId;
  }

  public String startZookeeperDetached() {
    try {
      return doStartZookeeperDetached();
    } catch (IOException | DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private String doStartZookeeperDetached() throws IOException, DockerException, InterruptedException {
    Path localZk = createTempDir("zk-tmp-dir");
    HostConfig.Builder hostConfig = HostConfig.builder()
        .portBindings(ZK_PORT_BINDING)
        .appendBinds(HostConfig.Bind.from(localZk.toAbsolutePath().toString()).to(ZK_DIR).build());

    ContainerConfig containerConfig = baseContainerConfig(hostConfig.build())
        .exposedPorts(ZK_PORT)
        .labels(ImmutableMap.of(DOCKER_LABEL, ""))
        .cmd(runZkCommand())
        .build();

    final ContainerCreation creation = newContainer(containerConfig);
    final String id = creation.id();
    docker.startContainer(id);

    waitZkUpWithLogging(id);
    new DockerCommandExecutor(docker, id).execute(SOLR_TO + "/server/scripts/cloud-scripts/zkcli.sh", "-zkhost", "localhost:2182", "-cmd", "makepath", "/solr");
    return id;
  }

  private void waitZkUpWithLogging(String id) throws DockerException, InterruptedException, IOException {
    try {
      waitZkUp();
    } catch (Exception e) {
      LOG.error("Emitting zookeeper stdout/err to stdout/err after failing to start up");
      LogStream logStream = docker.logs(id, stdout(), stderr(), tail(MAX_LOG_COUNT));
      logStream.attach(System.out, System.err, false);
      throw e;
    }
  }

  public String[] runZkCommand() {
    return new String[]{"java", "-classpath", SOLR_TO + "/dist/solrj-lib/*:/solr-to/server/lib/ext/*", ZooKeeperServerMain.class.getCanonicalName(), ZK_PORT, ZK_DIR};
  }

  public void waitZkUp() throws IOException {
    long before = System.nanoTime();
    int countDown = REQUIRED_SUCCESSFUL_CHECKS;
    while (countDown > 0) {
      Watcher watcher = System.out::println;
      ZooKeeper zk = new ZooKeeper("localhost:" + ZK_PORT, 30000, watcher);
      try {
        if (zk.exists("/", false) != null)
          //if (zk.getState().isConnected())
          LOG.info("Zookeeper is available, number of checks left:" + --countDown);
        else if (elapsedSecondsSince(before) > MAX_ZOOKEEPER_STARTUP_SECONDS)
          throw new RuntimeException("Zookeeper cannot be connected ");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (KeeperException e) {
        if (elapsedSecondsSince(before) > MAX_ZOOKEEPER_STARTUP_SECONDS)
          throw new RuntimeException("Zookeeper cannot be connected ", e);
      } finally {
        try {
          zk.close();
        } catch (InterruptedException e) {
          LOG.warn("Unable to close Zookeeper properly", e);
        }
      }
      sleep(ZOOKEEPER_PING_DELAY);
    }
    LOG.info("Zookeeper could be connected {} times, considered running", REQUIRED_SUCCESSFUL_CHECKS);
  }

  public void stop() {
    try {
      docker.killContainer(containerId);
      docker.waitContainer(containerId);
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
