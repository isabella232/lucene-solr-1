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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to build up a Solr CLI argument list
 */
class SolrArgsBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static String START = "start";
  private static String STOP = "stop";
  private static List<String> COMMANDS = Arrays.asList(START, STOP);

  private static final String FORCE = "-force";
  private static final String PORT = "-p";
  private static final String FOREGROUND = "-f";
  private static final String SOLRHOME_DIR_PARAM = "-s";
  private static final String ADDITIONAL_JVM_PARAMS = "-a";
  private static final String CLOUD_MODE_PARAM = "-cloud";
  private static final String ZK_HOST_PORT_PARAM = "-z";
  
  private static List<String> PARAMETERS = Arrays.asList(FORCE, PORT, FOREGROUND);

  private List<String> params = new ArrayList<>();

  public String getZkHost() {
    return zkHost;
  }

  private String zkHost;

  public SolrArgsBuilder(String solrDir) {
    params.add(solrDir + "/bin/solr");
  }

  public SolrArgsBuilder start() {
    verifyNoCommandYet();
    params.add(1, START);
    return this;
  }

  public SolrArgsBuilder stop() {
    verifyNoCommandYet();
    params.add(1, STOP);
    return this;
  }

  private void verifyNoCommandYet() {
    if (params.stream().anyMatch(x -> COMMANDS.contains(x))) {
      throw new IllegalStateException("Builder already have a command set.");
    }
  }

  public SolrArgsBuilder force() {
    params.add(FORCE);
    return this;
  }

  public SolrArgsBuilder coresDirWithExampleConfig(String dir) {
    workaroundToStartSolrWithExampleConfiguration();
    params.add(SOLRHOME_DIR_PARAM);
    params.add(dir);
    return this;
  }

  public SolrArgsBuilder home(String dir) {
    params.add(SOLRHOME_DIR_PARAM);
    params.add(dir);
    params.add(ADDITIONAL_JVM_PARAMS);
    params.add("-DcoreRootDirectory=" + dir);
    return this;
  }

  private void workaroundToStartSolrWithExampleConfiguration() {
    params.add("-e");
    params.add("default");
  }

  public SolrArgsBuilder port(String portNumber) {
    params.add(PORT);
    params.add(portNumber);
    return this;
  }

  public SolrArgsBuilder foreground() {
    params.add(FOREGROUND);
    return this;
  }

  public SolrArgsBuilder allInstances() {
    params.add("-all");
    return this;
  }

  /**
   * @param hdfsHostPort  In "host:port" format
   * @param homeDir Absolute path in HDFS
   */
  public SolrArgsBuilder withHdfs(String hdfsHostPort, String homeDir) {
    if (!params.contains(CLOUD_MODE_PARAM)) {
      LOG.warn("Cloud mode is required for use with HDFS");
    }
    params.add(ADDITIONAL_JVM_PARAMS);
    params.add("-Dsolr.lock.type=hdfs");
    params.add(ADDITIONAL_JVM_PARAMS);
    params.add("-Dsolr.directoryFactory=HdfsDirectoryFactory");
    params.add(ADDITIONAL_JVM_PARAMS);
    params.add("-Dsolr.hdfs.home=hdfs://" + hdfsHostPort + homeDir);
    return this;
  }

  public SolrArgsBuilder cloudMode(String zkHost) {
    this.zkHost = zkHost;
    params.add(CLOUD_MODE_PARAM);
    params.add(ZK_HOST_PORT_PARAM);
    params.add(zkHost);
    return this;
  }

  public String[] build() {
    return params.toArray(new String[params.size()]);
  }
}
