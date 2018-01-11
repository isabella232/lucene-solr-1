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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static java.nio.file.Files.readAllBytes;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.junit.matchers.JUnitMatchers.hasItem;

/**
 * Prerequisite: download tarball from
 * https://www.cloudera.com/documentation/enterprise/release-notes/topics/cm_vd_cdh_package_tarball_512.html#cm_vd_cdh_package_tarball_512
 * <p>
 * Useful commands to clean your docker env after failed test attempts:
 * <p>
 * #remove non-existing container from network, that would cause ports to seem already used.
 * docker network disconnect --force bridge runPreviousAndThenCurrentSolr2
 * <p>
 * #list all containters: docker container ls --all
 * #remove all containers: docker container prune -f
 */
public class DockerRunnerTest extends DockerRunnerTestBase {


  public static final int THREAD_GRACEFUL_CLOSE_TIMEOUT = 5;
  public static final int OK_RESPONSE = 0;
  private ExecutorService threadExecutor;
  private ZooKeeperServerMainWithoutExit zooKeeperServer;


  @Before
  public void setUp() throws Exception {
    super.setUp();
    threadExecutor = Executors.newSingleThreadExecutor();
  }

  @Test
  public void runSolrCloudOnRestartedZookeeper() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    zooKeeper.stop();
    zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    SolrCloudRunner solrCloudRunner = dockerRunner.solrCloudRunner(zooKeeper);

    copyMinFullSetup(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    solrCloudRunner.awaitSolr();
  }

  @Test
  public void runSolrInCloudMode() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    SolrCloudRunner solrCloudRunner = dockerRunner.solrCloudRunner(zooKeeper);

    copyMinFullSetup(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    solrCloudRunner.awaitSolr();
  }

  @Test
  public void runSolr4InCloudMode() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    solrCloudRunner.awaitSolr();
  }

  @Test
  public void collectionCanBeCreated() throws Exception {
    SolrCloudRunner solrCloudRunner = createSimpleDockerizedCluster();

    copyMinFullSetup(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    CollectionAdminResponse response = createCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME);

    assertConfigUploaded(response, CONFIG_NAME);
  }

  @Test
  public void collectionCanBeCreatedInSolr4() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);


    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    solrCloudRunner.awaitSolr();
    CollectionAdminResponse response = createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME_4_10_3);

    assertConfigUploaded(response, CONFIG_NAME_4_10_3);
  }

  @Test
  public void runLatestSolr() throws Exception {
    SolrRunner solrRunner = dockerRunner.solrRunner();
    solrRunner.start();
    solrRunner.awaitSolr();
  }

  @Test
  public void solrCanBeStartedAfterStopped() throws Exception {

    Solr4Runner solr4Runner = dockerRunner.solr4Runner();
    dockerRunner.copy4_10_3SolrXml(new File(solr4Runner.getNodeDir()));
    solr4Runner.start();
    solr4Runner.stop();
    solr4Runner.start();
  }


  @Test
  public void solrCannotBeStartedTwice() throws Exception {
    Solr4Runner solr4Runner = dockerRunner.solr4Runner();

    solr4Runner.start();
    try {
      solr4Runner.start();
      fail();
    } catch (DockerCommandExecutionException ignored) {
    }
  }

  @Test
  public void startZookeeperContainerAsDetachedForeground() throws Exception {
    dockerRunner.zooKeeperRunner().start();

  }
  
  @Test
  public void startHdfs() {
    HdfsRunner hdfsRunner = dockerRunner.hdfsRunner();

    hdfsRunner.start();
    
    DockerCommandExecutor executor = hdfsRunner.nameNodeExecutor();
    executor.execute("hdfs", "dfs", "-ls", "/");
    executor.execute("hdfs", "dfs", "-touchz", "/example");
  }

  @Test
  public void startSolrInCloudModeOnHdfs() throws Exception{
    ZooKeeperRunner zooKeeperRunner = dockerRunner.zooKeeperRunner();
    zooKeeperRunner.start();

    HdfsRunner hdfsRunner = dockerRunner.hdfsRunner();
    hdfsRunner.start();

    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeperRunner);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.startWithHdfs();
    solrCloudRunner.awaitSolr();

    DockerCommandExecutor executor = hdfsRunner.nameNodeExecutor();
    executor.execute("hdfs", "dfs", "-ls", "-R", "/");
  }

  @Test
  public void startZookeeperLocally() throws Exception {
    zooKeeperServer = new ZooKeeperServerMainWithoutExit();
    Path localZk = Files.createTempDirectory("zk-tmp-dir").toRealPath();
    threadExecutor.execute(() -> zooKeeperServer.initializeAndRun(new String[]{DockerRunner.ZK_PORT, localZk.toString()}));
    dockerRunner.zooKeeperRunner().waitZkUp();
  }

  @Test
  public void documentsCanBeAdded() throws Exception {
    SolrCloudRunner solrCloudRunner = createSimpleDockerizedCluster();

    copyMinFullSetup(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    CollectionAdminResponse response = createCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME);
    addDocument();
    assertThat(queryDocuments().get(0).get("id"), equalTo(ID_VALUE));
  }

  @Test
  public void documentsCanBeAddedToSolr4() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    CollectionAdminResponse response = createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME_4_10_3);
    addDocument();
    assertThat(queryDocuments().get(0).get("id"), equalTo(ID_VALUE));
  }

  @Test
  public void upgradeToolPartiallyValidatesSchema() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME_4_10_3);
    addDocument();
    Path schemaPath = solrCloudRunner.getSchemaCopy(COLLECTION_NAME);

    Path targetSchemaDir = Files.createTempDirectory("schema-tmp-dir").toRealPath();
    UpgradeToolUtil.doUpgradeSchema(schemaPath, targetSchemaDir);
    Path upgradeResult = Paths.get(targetSchemaDir.toString(), "schema_validation.xml");
    assertThat(new String(readAllBytes(upgradeResult)), not(containsBlockingIncompatibility()));
  }


  @Ignore("Index upgrade is not supported")
  @Test
  public void solr5UpgradeIndexExecutesSuccessfully() throws Exception {
    dockerRunner.buildImageWithPreviousSolrVersions();

    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME_4_10_3);
    //addDocument();
    solrCloudRunner.stop();
    Path index = solrCloudRunner.getIndexCopy();
    upgradeTool.upgradeIndex(index);
  }


  @Test
  public void upgradeToolUpgradesConfig() throws Exception {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    Solr4CloudRunner solrCloudRunner = dockerRunner.solr4CloudRunner(zooKeeper);

    dockerRunner.copy4_10_3SolrXml(new File(solrCloudRunner.getNodeDir()));
    solrCloudRunner.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONFIG_NAME_4_10_3);
    Path configPath = createTempDir("config-tmp").toRealPath();
    dockerRunner.downloadConfig(CONFIG_NAME_4_10_3, configPath);

    Path targetConfigDir = Files.createTempDirectory("config-tmp-dir").toRealPath();
    UpgradeToolUtil.doUpgradeConfig(configPath.resolve("solrconfig.xml"), targetConfigDir);
    Path upgradeResult = Paths.get(targetConfigDir.toString(), "solrconfig_validation.xml");
    assertThat(new String(readAllBytes(upgradeResult)), not(containsBlockingIncompatibility()));
  }


  private class ZooKeeperServerMainWithoutExit extends ZooKeeperServerMain {

    @Override
    public void initializeAndRun(String[] args) {
      try {
        super.initializeAndRun(args);
      } catch (QuorumPeerConfig.ConfigException | IOException e) {
        throw new RuntimeException(e);
      }
    }
    @Override
    public void shutdown() {
      super.shutdown();
    }

  }

  private void assertConfigUploaded(CollectionAdminResponse response, String configName) throws SolrServerException, IOException {
    assertEquals(OK_RESPONSE, response.getStatus());
    assertTrue(response.getErrorMessages() == null ? "no error from server" : response.getErrorMessages().toString(),
        response.isSuccess());
    assertThat(new ConfigSetAdminRequest.List().process(cloudClient).getConfigSets(), hasItem(configName));
  }

  private SolrCloudRunner createSimpleDockerizedCluster() {
    ZooKeeperRunner zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    return dockerRunner.solrCloudRunner(zooKeeper);
  }


  @After
  public void closeAll() throws Exception {
    threadExecutor.shutdownNow();
    threadExecutor.awaitTermination(THREAD_GRACEFUL_CLOSE_TIMEOUT, TimeUnit.SECONDS);
    if (zooKeeperServer != null) {
      zooKeeperServer.shutdown();
    }
    if (cloudClient != null) {
      cloudClient.close();
    }
  }


}
