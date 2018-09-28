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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Before;

/**
 * Base class to subclass when adding Docker based Solr tests
 */
public class UpgradeTestBase extends DockerRunnerTestBase {

  Solr4CloudRunner solr4 = null;
  SolrCloudRunner solr = null;
  Path upgradedDir;
  private ZooKeeperRunner zooKeeper;

  static final String INCOMPATIBILITY_DESCRIPTIONS = "table.table-%s tbody tr td.description";

  @Before
  public void setUp() throws Exception {
    super.setUp();

    upgradedDir = createTempDir("tmp-converted");
    zooKeeper = dockerRunner.zooKeeperRunner();
    UpgradeToolUtil.init();
  }

  void reCreateCollection(String configName) throws IOException, SolrServerException {
    dockerRunner.uploadConfig(upgradedDir, configName);
    createCollection(COLLECTION_NAME, configName);
  }

  void provisionSolrXml() throws IOException {
    File nodeDir = new File(solr.getNodeDir());
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml");
    FileUtils.copyFile(xmlF, new File(nodeDir, "solr.xml"));
  }

  void createCurrentSolrCluster() {
    zooKeeper = dockerRunner.zooKeeperRunner();
    zooKeeper.start();
    solr = dockerRunner.solrCloudRunner(zooKeeper);
    try {
      copyMinFullSetup(new File(solr.getNodeDir()));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage());
      throw new RuntimeException(e);
    }

  }


  void stopSolr4() {
    solr4.stop();
    zooKeeper.stop();
  }


  void upgradeConfigs(String configName) throws IOException {
    upgradeConfig(configName);
    upgradeSchema();
  }

  void upgradeConfig(String configName) throws IOException {
    upgradeConfig(configName, false);
  }

  void upgradeConfig(String configName, boolean dryRun) throws IOException {
    Path solrConfig = downLoadConfig(configName);
    UpgradeToolUtil.doUpgradeConfig(solrConfig, upgradedDir, dryRun);
    LOG.info("converted solrconfig.xml:");
    LOG.info(new String(Files.readAllBytes(upgradedDir.resolve("solrconfig.xml"))));
    Files.delete(upgradedDir.resolve("solrconfig_validation.html"));
  }

  void upgradeSchema(String configName, boolean dryRun) throws IOException {
    Path schemaPath = solr4.getSchemaCopy(configName);
    UpgradeToolUtil.doUpgradeSchema(schemaPath, upgradedDir, dryRun);
    LOG.info("converted schema.xml:");
    LOG.info(new String(Files.readAllBytes(upgradedDir.resolve("schema.xml"))));
    Files.delete(upgradedDir.resolve("schema_validation.html"));
  }

  void upgradeSchema() throws IOException {
    upgradeSchema(COLLECTION_NAME, false);
  }

  void createSolr4Cluster() {
    zooKeeper.start();
    solr4 = dockerRunner.solr4CloudRunner(zooKeeper);
  }

  void assertConfigMigration(String configName) throws IOException, SolrServerException {
    createSolr4Cluster();

    dockerRunner.copy4_10_3SolrXml(new File(solr4.getNodeDir()));
    solr4.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, configName);

    upgradeConfigs(configName);

    stopSolr4();

    createCurrentSolrCluster();

    provisionSolrXml();
    solr.start();


    reCreateCollection(configName);
  }

  Set<String> getIncompatibilitiesByQuery(String query, Path input) throws  IOException {
    Document result = Jsoup.parse(input.toFile(), "utf-8");
    Elements errors = result.select(query);
    return errors.stream().map(UpgradeTestBase::extractWholeText).collect(Collectors.toSet());
  }

  private static String extractWholeText(Element element) {
    return element.wholeText();
  }
}
