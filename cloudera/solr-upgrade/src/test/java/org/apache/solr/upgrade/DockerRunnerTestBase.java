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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

import com.spotify.docker.client.DockerClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.allOf;
import static org.junit.matchers.JUnitMatchers.containsString;


/**
 * Base class for docker based infrastructure
 */
public class DockerRunnerTestBase extends SolrTestCaseJ4 {
  public static final String CONFIG_NAME_4_10_3 = "cloud-plain-4-10-3";
  public static final String CONFIG_NAME = "cloud-plain";
  public static final String COLLECTION_NAME = "solrj_collection";
  public static final String ID_VALUE = "1000";
  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int MAX_ROWS = 10;
  protected static DockerClient docker;
  protected DockerRunner dockerRunner;
  protected UpgradeToolFacade upgradeTool;

  @Rule
  public TestName name = new TestName();


  @BeforeClass
  public static void setUpClassBase() throws Exception {
    docker = DockerRunner.dockerClient();
    if (System.getProperty("skipImageBuilding") == null) {
      new DockerRunner.Context().build().buildImages();
    }

  }

  @Before
  public void setUpBase() throws Exception {
    DockerRunner.removeAllDockerContainers();
    dockerRunner = new DockerRunner.Context()
        .withContainerNamePrefix(name.getMethodName())
        .build();
    upgradeTool = dockerRunner.upgradeToolFacade();
  }

  protected CloudSolrClient localCloudClient() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    return new CloudSolrClient.Builder()
        .withZkChroot("/solr")
        .withZkHost("localhost" + ":" + DockerRunner.ZK_PORT)
        .withHttpClient(httpClient)
        .build();
  }

  @After
  public void afterMethod() throws Exception {
    if (skipTestCleanup()) {
      LOG.info("Skipping test cleanup");
      return;
    }
    DockerRunner.removeAllDockerContainers();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (skipTestCleanup())
      LOG.info("Skipping test class cleanup");
    else
      DockerRunner.removeAllDockerContainers();
    DockerRunner.closeAllClients();
  }

  private static boolean skipTestCleanup() {
    return System.getProperties().containsKey("skipCleanup");
  }

  protected CollectionAdminResponse createCollectionBasedOnConfig(String collectionName, String configName, Path configDir) throws IOException, SolrServerException {
    dockerRunner.uploadConfig(configDir, configName);
    return createCollection(collectionName, configName);
  }
  
  protected CollectionAdminResponse createCollectionBasedOnConfig(String collectionName, String configName) throws IOException, SolrServerException {
    return createCollectionBasedOnConfig(collectionName, configName, configset(configName));
  }

  protected CollectionAdminResponse createLegacyCollectionBasedOnConfig(String collectionName, String configName) throws IOException, SolrServerException {
    return createCollectionBasedOnConfig(collectionName, configName, TEST_PATH().resolve("configsets").resolve("legacy4_10_3").resolve(configName).resolve("conf"));
  }

  protected CollectionAdminResponse createCollection(String collectionName, String configName) throws SolrServerException, IOException {
    try (CloudSolrClient client = localCloudClient()) {
      CollectionAdminRequest.Create createCollectionRequest = CollectionAdminRequest.createCollection(collectionName, configName, 1, 1)
          .setRouterField("myOwnField");
      CollectionAdminResponse response = createCollectionRequest.process(client);
      assertTrue(response.getErrorMessages() == null ? "no error returned from server" : response.getErrorMessages().toString(),
          response.isSuccess());
      return response;
    }
  }



  protected Path downLoadConfig(String configName) throws IOException {Path configPath = createTempDir("config-tmp").toRealPath();
    dockerRunner.downloadConfig(configName, configPath);
    return configPath.resolve("solrconfig.xml");
  }

  protected SolrDocumentList queryDocuments() throws SolrServerException, IOException {
    try(CloudSolrClient client = localCloudClient()) {
      SolrQuery query = new SolrQuery("*:*").setRows(MAX_ROWS);
      query.setParam(UpdateParams.COLLECTION, COLLECTION_NAME);
      QueryRequest queryRequest = new QueryRequest(query);
      QueryResponse qRes = queryRequest.process(client);
      return qRes.getResults();
    }
  }

  protected void addDocument() throws SolrServerException, IOException {
    try(CloudSolrClient client = localCloudClient()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", DockerRunnerTestBase.ID_VALUE);
      doc.addField("myOwnField", "value1");
      UpdateRequest add = new UpdateRequest()
          .add(doc);
      add.setParam(UpdateParams.COLLECTION, COLLECTION_NAME);

      add.process(client);
      client.commit(COLLECTION_NAME);
    }
  }

  protected Matcher<String> containsBlockingIncompatibility() {
    return allOf(containsString("<level>error</level>"), containsString("incompatibility"));
  }
}
