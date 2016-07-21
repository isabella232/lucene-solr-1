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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress test LiveNodes watching.
 *
 * Does bursts of adds to live_nodes using parallel threads to and verifies that after each 
 * burst a ZkStateReader detects the correct set.
 */
@Slow
public class TestReloadManyReplicas extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** A basic cloud client */
  private static CloudSolrServer CLOUD_CLIENT;
  
  /* how many seconds we're willing to wait for our executor tasks to finish before failing the test */
  private final static int WAIT_TIME = TEST_NIGHTLY ? 60 : 30;

  /** default collection **/
  private final static String collection = "solrj_collection";

  // Default solr.xml except with a much higher leader wait timeout
  private static final String SOLR_XML = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace("10000", "180000");
  
  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // we only need 1 node because we're going to be shoving all of the replicas on there to promote deadlock
    configureCluster(1)
    .addConfig("conf1", configset("cloud-minimal"))
    .withSolrXml(SOLR_XML)
    .configure();

    // give all nodes a chance to come alive
//    TestTolerantUpdateProcessorCloud.assertSpinLoopAllJettyAreRunning(cluster);
    
    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.connect(); // force connection even though we aren't sending any requests
    CLOUD_CLIENT.setDefaultCollection(collection);

    DirectUpdateHandler2.commitOnClose = false;
    
    useFactory(null);
  }
  
  @AfterClass
  private static void afterClass() throws Exception {
    CLOUD_CLIENT.shutdown();
    CLOUD_CLIENT = null;

    DirectUpdateHandler2.commitOnClose = true;
    
    resetFactory();
  }

  public void testStress() throws Exception {
    Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(collection);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setNumShards(1);
    createCollectionRequest.setReplicationFactor(ConfigSolr.DEFAULT_CORE_LOAD_THREADS_IN_CLOUD * 3);
    createCollectionRequest.setMaxShardsPerNode(Integer.MAX_VALUE);
    createCollectionRequest.setRouterField("id");
    CollectionAdminResponse collectionResponse = createCollectionRequest.process(CLOUD_CLIENT);

    assertEquals(0, collectionResponse.getStatus());
    assertTrue(collectionResponse.isSuccess());

    assertEquals(1, cluster.getJettySolrRunners().size());

    log.info("Adding Docs!");
    int numDocs = 100;
    for (int docId = 1; docId <= numDocs; docId++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docId);
      doc.addField("text", "shard" + docId % 5);
      CLOUD_CLIENT.add(doc);
    }

    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
    countDocs(numDocs);

    printLeader();
    
    log.info("Activating the Chaos Monkey!");
    ChaosMonkey monkey = new ChaosMonkey(cluster.getZkServer(), CLOUD_CLIENT.getZkStateReader(), collection, null, null);
    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0); 
    monkey.stopJetty(jetty);
    monkey.start(jetty);

    printLeader();
    
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, CLOUD_CLIENT.getZkStateReader(), true, true, WAIT_TIME);

    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
    countDocs(numDocs);
  }

  private void printLeader() throws InterruptedException {
    Replica leader = CLOUD_CLIENT.getZkStateReader().getLeaderRetry(collection, "shard1");
    log.info("Leader is : {}/{}", leader.getNodeName(), leader.getCoreName());
  }

  private void countDocs(int expected) throws SolrServerException, IOException {
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "*:*");
    QueryResponse queryResponse = CLOUD_CLIENT.query(queryParams);

    assertEquals(0, queryResponse.getStatus());
    assertEquals(expected, queryResponse.getResults().getNumFound());
  }

}
