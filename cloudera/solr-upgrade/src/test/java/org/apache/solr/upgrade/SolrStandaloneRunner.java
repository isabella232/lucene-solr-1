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

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;

import static org.apache.solr.upgrade.DockerRunner.SOLR_LOG_LOCATION;
import static org.apache.solr.upgrade.DockerRunner.SOLR_PORT;
import static org.apache.solr.upgrade.DockerRunner.SOLR_TO;

/**
 * Start/stop Solr standalone
 */
public class SolrStandaloneRunner extends AbstractRunner implements SolrRunner {

  private String solrContainer;


  public SolrStandaloneRunner(DockerRunner.Context context) {
    super(context);
  }

  public void start() {
    SolrArgsBuilder runArgs = standaloneRunArgs(solrStartArgs(SOLR_TO));
    runSolr(runArgs);
  }

  private void runSolr(SolrArgsBuilder runArgs) {
    HostConfig.Builder hostConfig = HostConfig.builder()
        .portBindings(DockerRunner.SOLR_PORT_BINDINGS);

    ContainerConfig container = baseContainerConfig(hostConfig.build())
        .exposedPorts(SOLR_PORT)
        .labels(ImmutableMap.of(DockerRunner.DOCKER_LABEL, ""))
        .cmd(runArgs.build())
        .attachStderr(true)
        .attachStdout(true)
        .tty(true)
        .build();

    solrContainer = runInContainer(container);
    waitSolrUp(new DockerCommandExecutor(docker, solrContainer), SOLR_TO + SOLR_LOG_LOCATION);

  }

  @Override
  public void dumpLogFileIfPossible() {
    dumpSolrLogIfPossible(new DockerCommandExecutor(docker, solrContainer), SOLR_TO + SOLR_LOG_LOCATION);

  }


  private SolrArgsBuilder standaloneRunArgs(SolrArgsBuilder solrArgsBuilder) {
    return solrArgsBuilder.force();
  }

  public void stop() {
    stopSolr4(new DockerCommandExecutor(docker, solrContainer));
  }

  private SolrArgsBuilder solrStartArgs(String solrDir) {
    return new SolrArgsBuilder(solrDir).start().port(SOLR_PORT).foreground();
  }


}
