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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.config.upgrade.ConfigUpgradeTool;
import org.junit.Before;

public class UpgradeTestBase extends DockerRunnerTestBase {

  protected Solr4CloudRunner solr4;
  protected SolrCloudRunner solr;
  private Path upgradedDir;
  private ZooKeeperRunner zooKeeper;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    upgradedDir = createTempDir("tmp-converted");
    zooKeeper = dockerRunner.zooKeeperRunner();
    UpgradeToolUtil.init();
  }

  protected void reCreateCollection(String configName) throws IOException, SolrServerException {
    dockerRunner.uploadConfig(upgradedDir, configName);
    createCollection(COLLECTION_NAME, configName);
  }

  protected void provisionSolrXml() throws IOException {
    File nodeDir = new File(solr.getNodeDir());
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml");
    FileUtils.copyFile(xmlF, new File(nodeDir, "solr.xml"));
  }

  protected void createCurrentSolrCluster() {
    zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    solr = dockerRunner.solrCloudRunner(zooKeeper);
  }


  protected void stopSolr4() {
    closeClient();
    solr4.stop();
    zooKeeper.stop();
  }

  private void closeClient() {
    try {
      cloudClient.close();
      cloudClient = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void upgradeConfigs(String configName) throws IOException {
    Path solrConfig = downLoadConfig(configName);
    Path schemaPath = solr4.getSchemaCopy(COLLECTION_NAME);
    UpgradeToolUtil.doUpgradeConfig(solrConfig, upgradedDir);
    UpgradeToolUtil.doUpgradeSchema(schemaPath, upgradedDir);
    LOG.info("converted schema.xml:");
    LOG.info(new String(Files.readAllBytes(upgradedDir.resolve("schema.xml"))));
    LOG.info("converted solrconfig.xml:");
    LOG.info(new String(Files.readAllBytes(upgradedDir.resolve("solrconfig.xml"))));
    Files.delete(upgradedDir.resolve("solrconfig_validation.xml"));
    Files.delete(upgradedDir.resolve("schema_validation.xml"));
  }

  protected void createSolr4Cluster() {
    zooKeeper.start();
    solr4 = dockerRunner.solr4CloudRunner(zooKeeper);
  }

  protected void assertConfigMigration(String configName) throws IOException, SolrServerException {
    createSolr4Cluster();

    dockerRunner.copy4_10_3SolrXml(new File(solr4.getNodeDir()));
    solr4.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, configName);

    upgradeConfigs(configName);

    stopSolr4();

    createCurrentSolrCluster();

    provisionSolrXml();
    solr.start();
    resetCloudClient();


    reCreateCollection(configName);
  }
}
