package org.apache.solr.cloud;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase.CloudSolrServerClient;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniSolrCloudCluster {
  
  private static Logger log = LoggerFactory.getLogger(MiniSolrCloudCluster.class);

  public static final String DEFAULT_CLOUD_SOLR_XML = "<solr>\n" +
      "\n" +
      "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
      "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
      "\n" +
      "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
      "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
      "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
      "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
      "  </shardHandlerFactory>\n" +
      "\n" +
      "  <solrcloud>\n" +
      "    <str name=\"host\">127.0.0.1</str>\n" +
      "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
      "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">10000</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "  </solrcloud>\n" +
      "  \n" +
      "</solr>\n";

  private ZkTestServer zkServer;
  private final boolean externalZkServer;
  private List<JettySolrRunner> jettys;
  private final CloudSolrServer solrClient;
  private final AtomicInteger nodeIds = new AtomicInteger();
  private final Path baseDir;

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    this(numServers, hostContext, LuceneTestCase.createTempDir().toPath(), solrXml,
        extraServlets, extraRequestFilters, null);
  }

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    this(numServers, hostContext, baseDir, solrXml, extraServlets, extraRequestFilters, null);
  }

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters,
      SSLConfig sslConfig) throws Exception {
    this(numServers, hostContext, baseDir, solrXml, extraServlets, extraRequestFilters, sslConfig, null);
  }

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @param zkTestServer ZkTestServer to use.  If null, one will be created
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters,
      SSLConfig sslConfig,
      ZkTestServer zkTestServer) throws Exception {

    this.baseDir = baseDir;

    try (InputStream is = new FileInputStream(solrXml)) {
      init(numServers, hostContext, IOUtils.toByteArray(is), extraServlets, extraRequestFilters, sslConfig, zkTestServer);
    }

    this.externalZkServer = zkTestServer != null;
    this.solrClient = new CloudSolrServer(getZkServer().getZkAddress());
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   *
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig) throws Exception {
    this(numServers, baseDir, solrXml, jettyConfig, null);
  }

  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig, ZkTestServer zkTestServer) throws Exception {
    this(numServers, jettyConfig.context, baseDir, solrXml, new TreeMap<>(jettyConfig.extraServlets),
        new TreeMap<Class, String>(jettyConfig.extraFilters), jettyConfig.sslConfig, zkTestServer);
  }

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @param zkTestServer ZkTestServer to use.  If null, one will be created
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, Path baseDir, String solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters,
      SSLConfig sslConfig,
      ZkTestServer zkTestServer) throws Exception {
    this.baseDir = baseDir;

    init(numServers, hostContext, solrXml.getBytes(Charset.defaultCharset()), extraServlets, extraRequestFilters, sslConfig, zkTestServer);
    this.externalZkServer = zkTestServer != null;
    this.solrClient = new CloudSolrServer(getZkServer().getZkAddress());
  }

  public void init(int numServers, String hostContext, byte[] solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters,
      SSLConfig sslConfig,
      ZkTestServer zkTestServer) throws Exception {


    if (zkTestServer == null) {
      String zkDir = baseDir.resolve("zookeeper/server1/data").toString();
      zkTestServer = new ZkTestServer(zkDir);
      zkTestServer.run();
    }
    this.zkServer = zkTestServer;

    SolrZkClient zkClient = null;
    try {
      zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, 45000, null);
      zkClient.makePath("/solr", false, true);
      zkClient.create("/solr/solr.xml", solrXml, CreateMode.PERSISTENT, true);
    } finally {
      if (zkClient != null) zkClient.close();
    }

    // tell solr to look in zookeeper for solr.xml
    System.setProperty("solr.solrxml.location","zookeeper");
    System.setProperty("zkHost", zkServer.getZkAddress());

    jettys = new LinkedList<JettySolrRunner>();
    for (int i = 0; i < numServers; ++i) {
      if (sslConfig == null) {
        startJettySolrRunner(newNodeName(), hostContext, extraServlets, extraRequestFilters);
      } else {
        startJettySolrRunner(newNodeName(), hostContext, extraServlets, extraRequestFilters, sslConfig);
      }
    }
  }

  public String newNodeName() {
    return "node" + nodeIds.incrementAndGet();
  }

  private Path createInstancePath(String name) throws IOException {
    Path instancePath = baseDir.resolve(name);
    Files.createDirectories(instancePath);
    return instancePath;
  }

  /**
   * @return ZooKeeper server used by the MiniCluster
   */
  public ZkTestServer getZkServer() {
    return zkServer;
  }

  /**
   * @return Unmodifiable list of all the currently started Solr Jettys.
   */
  public List<JettySolrRunner> getJettySolrRunners() {
    return Collections.unmodifiableList(jettys);
  }

  /**
   * Start a new Solr instance
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    return startJettySolrRunner(name, hostContext, extraServlets, extraRequestFilters, null);
  }

  /**
   * Start a new Solr instance
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters, SSLConfig sslConfig) throws Exception {
    Path runnerPath = createInstancePath(name);
    String context = getHostContextSuitableForServletContext(hostContext);
    JettySolrRunner jetty = new JettySolrRunner(runnerPath.toString(), context,
      0, null, null, true, extraServlets, sslConfig, extraRequestFilters);
    jetty.start();
    jettys.add(jetty);
    return jetty;
  }

  /**
   * Stop a Solr instance
   * @param index the index of node in collection returned by {@link #getJettySolrRunners()}
   * @return the shut down node
   */
  public JettySolrRunner stopJettySolrRunner(int index) throws Exception {
    JettySolrRunner jetty = jettys.get(index);
    jetty.stop();
    jettys.remove(index);
    return jetty;
  }

  public void uploadConfigDir(File configDir, String configName) throws IOException, KeeperException, InterruptedException {
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {
      ZkConfigManager manager = new ZkConfigManager(zkClient);
      manager.uploadConfigDir(configDir.toPath(), configName);
    }
  }

  public CloudSolrServer getSolrClient() {
    return solrClient;
  }

  public NamedList<Object> createCollection(String name, int numShards, int replicationFactor,
      String configName, Map<String, String> collectionProperties) throws SolrServerException, IOException {
    return createCollection(name, numShards, replicationFactor, configName, null, collectionProperties);
  }

  public NamedList<Object> createCollection(String name, int numShards, int replicationFactor,
      String configName, String createNodeSet, Map<String, String> collectionProperties) throws SolrServerException, IOException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
    params.set(CoreAdminParams.NAME, name);
    params.set("numShards", numShards);
    params.set("replicationFactor", replicationFactor);
    params.set("collection.configName", configName);
    if (null != createNodeSet) {
      params.set(OverseerCollectionMessageHandler.CREATE_NODE_SET, createNodeSet);
    }
    if(collectionProperties != null) {
      for(Map.Entry<String, String> property : collectionProperties.entrySet()){
        params.set(CoreAdminParams.PROPERTY_PREFIX + property.getKey(), property.getValue());
      }
    }

    return makeCollectionsRequest(params);
  }

  public NamedList<Object> deleteCollection(String name) throws SolrServerException, IOException {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CollectionAction.DELETE.name());
    params.set(CoreAdminParams.NAME, name);

    return makeCollectionsRequest(params);
  }

  private NamedList<Object> makeCollectionsRequest(final ModifiableSolrParams params) throws SolrServerException, IOException {

    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    return solrClient.request(request);
  }

  /**
   * Shut down the cluster, including all Solr nodes and ZooKeeper
   */
  public void shutdown() throws Exception {
    try {
      if (solrClient != null) {
        solrClient.shutdown();
      }
      for (int i = jettys.size() - 1; i >= 0; --i) {
        stopJettySolrRunner(i);
      }
    } finally {
      try {
        if (!externalZkServer) {
          zkServer.shutdown();
        }
      } finally {
        System.clearProperty("solr.solrxml.location");
        System.clearProperty("zkHost");
      }
    }
  }

  private static String getHostContextSuitableForServletContext(String ctx) {
    if (ctx == null || "".equals(ctx)) ctx = "/solr";
    if (ctx.endsWith("/")) ctx = ctx.substring(0,ctx.length()-1);;
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    return ctx;
  }

  /**
   * Return the jetty that a particular replica resides on
   */
  public JettySolrRunner getReplicaJetty(Replica replica) {
    for (JettySolrRunner jetty : jettys) {
      if (replica.getCoreUrl().startsWith(jetty.getBaseUrl().toString()))
        return jetty;
    }
    throw new IllegalArgumentException("Cannot find Jetty for a replica with core url " + replica.getCoreUrl());
  }

  /**
   * @return a randomly-selected Jetty
   */
  public JettySolrRunner getRandomJetty(Random random) {
    int index = random.nextInt(jettys.size());
    return jettys.get(index);
  }
}