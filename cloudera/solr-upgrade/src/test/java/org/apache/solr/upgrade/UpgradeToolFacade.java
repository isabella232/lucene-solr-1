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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;

import static com.spotify.docker.client.DockerClient.LogsParam.follow;
import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static java.nio.file.Files.readAllBytes;
import static org.apache.solr.upgrade.DockerRunner.DOCKER_LABEL;
import static org.apache.solr.upgrade.DockerRunner.IMAGE_WITH_SOLR5_AND_6;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

public class UpgradeToolFacade extends AbstractRunner {
  public UpgradeToolFacade(DockerRunner.Context context) {
    super(context);
  }

  public void upgradeConfig(Path configPath, Path targetConfigDir) {
    String upgradeToolDirString = "solr/build/solr-core/upgrade-tool/solr-upgrade-tool-0.0.1-SNAPSHOT/";
    Path upgradeToolDir = Paths.get(upgradeToolDirString).toAbsolutePath();
    assertTrue("Upgrade tool dir should exist: " + upgradeToolDir, Files.exists(upgradeToolDir));

    final File outputFile = new File(upgradeToolDirString, "config_upgrade.sh");
    outputFile.setExecutable(true, false);

    HostConfig.Builder hostConfig = HostConfig.builder()
        .appendBinds(HostConfig.Bind.from(configPath.toString()).to("/config-dir/solrconfig.xml").readOnly(true).build())
        .appendBinds(HostConfig.Bind.from(targetConfigDir.toString()).to(WORK_DIR).build())
        .appendBinds(HostConfig.Bind.from(upgradeToolDir.toString()).to("/upgrade-tool").build());
    ContainerConfig containerConfig = baseContainerConfig(hostConfig.build())
        .labels(ImmutableMap.of(DOCKER_LABEL, ""))
        .cmd("/upgrade-tool/config_upgrade.sh", "-c", "/config-dir/" + configPath.getFileName().toString(), "-t", "solrconfig", "-u", "/upgrade-tool/validators/solr_4_to_5_processors.xml", "-d", WORK_DIR)
        .build();

    execute(containerConfig);
    Path upgradeResult = Paths.get(targetConfigDir.toString(), "solrconfig_validation.xml");
    assertThat(new String(readAll(upgradeResult)), not(containsString("<level>error</level>")));
  }

  public static byte[] readAll(Path path) {
    try {
      return readAllBytes(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void upgradeSchema(Path schemaPath, Path targetSchemaDir) {

    try {
      doUpgradeSchema(schemaPath, targetSchemaDir);
    } catch (DockerException | InterruptedException | URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }

  }

  private void doUpgradeSchema(Path schemaPath, Path targetSchemaDir) throws DockerException, InterruptedException, URISyntaxException, IOException {
    String upgradeToolDirString = "solr/build/solr-core/upgrade-tool/solr-upgrade-tool-0.0.1-SNAPSHOT/";
    Path upgradeToolDir = Paths.get(upgradeToolDirString).toAbsolutePath();
    assertTrue("Upgrade tool dir should exist: " + upgradeToolDir, Files.exists(upgradeToolDir));

    // ant untar task doesn't retain file permissions, so we're making sure the upgrade tool is executable
    final File outputFile = new File(upgradeToolDirString, "config_upgrade.sh");
    outputFile.setExecutable(true, false);

    HostConfig.Builder hostConfig = HostConfig.builder()
        .appendBinds(HostConfig.Bind.from(schemaPath.getParent().toString()).to("/schema-dir").readOnly(true).build())
        .appendBinds(HostConfig.Bind.from(targetSchemaDir.toString()).to(WORK_DIR).build())
        .appendBinds(HostConfig.Bind.from(upgradeToolDir.toString()).to("/upgrade-tool").build());
    ContainerConfig containerConfig = baseContainerConfig(hostConfig.build())
        .labels(ImmutableMap.of(DOCKER_LABEL, ""))
        .cmd("/upgrade-tool/config_upgrade.sh", "-c", "/schema-dir/schema.xml", "-t", "schema", "-u", "/upgrade-tool/validators/solr_4_to_5_processors.xml", "-d", WORK_DIR)
        .build();

    execute(containerConfig);
  }


  private void execute(ContainerConfig containerConfig) {
    try {
      ContainerCreation container = newContainer(containerConfig);
      startContainer(container);
      LogStream logStream = null;
      logStream = docker.logs(container.id(), follow(), stdout(), stderr());
      logStream.attach(System.out, System.err, false);
      ContainerExit exitInfo = docker.waitContainer(container.id());
      if(exitInfo.statusCode() != 0) {
        throw new RuntimeException("Docker command executed with failure, code: " + exitInfo.statusCode());
      }
    } catch (DockerException | IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void startContainer(ContainerCreation container) {
    try {
      docker.startContainer(container.id());
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void upgradeIndex(Path index) {
    HostConfig hostConfig = HostConfig.builder()
        .appendBinds(HostConfig.Bind.from(index.toString()).to(WORK_DIR).build())
        .build();
    ContainerConfig containerConfig = ContainerConfig.builder()
        .hostConfig(hostConfig)
        .image(IMAGE_WITH_SOLR5_AND_6)
        .cmd("/bin/upgrade-to-5.sh", WORK_DIR+"/index")
        .labels(ImmutableMap.of(DOCKER_LABEL, ""))
        .build();
    execute(containerConfig);
  }


}
