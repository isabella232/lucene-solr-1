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

import static org.apache.solr.common.util.Utils.makeMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@Slow
@SuppressSSL
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class SharedFSAutoReplicaFailoverTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final boolean DEBUG = true;
  public static final int FULL_RELOAD_TIME = 320000;
  private static MiniDFSCluster dfsCluster;

  ThreadPoolExecutor executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  private final Map<String, String> collectionUlogDirMap = new HashMap<>();

  
  @BeforeClass
  public static void hdfsFailoverBeforeClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "512");
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
    System.setProperty("solr.hdfs.blockcache.global", "true"); // always use global cache, this test can create a lot of directories
    schemaString = "schema15.xml"; 
    System.setProperty("autoReplicaFailoverWorkLoopDelay", "500");
    System.setProperty("autoReplicaFailoverWaitAfterExpiration", "15000");
  }
  
  @AfterClass
  public static void hdfsFailoverAfterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.blockcache.blocksperbank");
    dfsCluster = null;
    System.clearProperty("autoReplicaFailoverWorkLoopDelay");
    System.clearProperty("autoReplicaFailoverWaitAfterExpiration");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionUlogDirMap.clear();
    // AutoAddRepliacs only works with legacyCloud=false
    CollectionAdminRequest.setClusterProperty("legacyCloud", "false").process(cloudClient);
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useJettyDataDir = false;
  }
  
  protected String getSolrXml() {
    return "solr.xml";
  }

  
  public SharedFSAutoReplicaFailoverTest() {
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    try {
      // to keep uncommitted docs during failover
      DirectUpdateHandler2.commitOnClose = false;
      testBasics();
    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      if (DEBUG) {
        super.printLayout();
      }
    }
  }

  // very slow tests, especially since jetty is started and stopped
  // serially
  private void testBasics() throws Exception {
    int highShards = 10;
    
    String collection1 = "solrj_collection";
    Create createCollectionRequest = CollectionAdminRequest.createCollection(collection1,"conf1",2,2)
            .setMaxShardsPerNode(15)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection1, 4, FULL_RELOAD_TIME);
    
    String collection2 = "solrj_collection2";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection2,"conf1",2,2)
            .setMaxShardsPerNode(2)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(false);
    CollectionAdminResponse response2 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response2.getStatus());
    assertTrue(response2.isSuccess());
    
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection2, 4, FULL_RELOAD_TIME);
    
    String collection3 = "solrj_collection3";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection3, "conf1", highShards, 1)
            .setMaxShardsPerNode(15)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response3 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response3.getStatus());
    assertTrue(response3.isSuccess());
    
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection3, highShards, FULL_RELOAD_TIME);

    // a collection has only 1 replica per a shard
    String collection4 = "solrj_collection4";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection4, "conf1", highShards, 1)
        .setMaxShardsPerNode(100)
        .setRouterField("text")
        .setAutoAddReplicas(true);
    CollectionAdminResponse response4 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response4.getStatus());
    assertTrue(response4.isSuccess());

    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection4, highShards, FULL_RELOAD_TIME);

    // all collections
    String[] collections = {collection1, collection2, collection3, collection4};

    // add some documents to collection4
    final int numDocs = 100;
    addDocs(collection4, numDocs, false);  // indexed but not committed

    // no result because not committed yet
    queryAndAssertResultSize(collection4, 0, 10000);

    assertUlogDir(collections);

    cloudClient.setDefaultCollection(collection4);
    cloudClient.commit(); // to query all docs

    killJetty(1);
    killJetty(2);

    Thread.sleep(5000);

    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection1, FULL_RELOAD_TIME);
    
    assertSliceAndReplicaCount(collection1, 2);

    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection2) < 4);

    // collection3 has maxShardsPerNode=1, there are 4 standard jetties and one control jetty and 2 nodes stopped
    ClusterStateUtil.waitForReplicasUp(cloudClient.getZkStateReader(), collection3, highShards, FULL_RELOAD_TIME);

    // collection4 has maxShardsPerNode=5 and setMaxShardsPerNode=5
    ClusterStateUtil.waitForReplicasUp(cloudClient.getZkStateReader(), collection4, highShards, FULL_RELOAD_TIME);

    // all docs should be queried after failover

    Thread.sleep(2000);

    ClusterStateUtil.waitForReplicasUp(
        cloudClient.getZkStateReader(), collection4, highShards, 30000);

    assertSingleReplicationAndShardSize(collection4, highShards);
    queryAndAssertResultSize(collection4, numDocs, 10000);
    
    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection1, FULL_RELOAD_TIME);

    Thread.sleep(2000);
    
    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(
            cloudClient.getZkStateReader(), collection1));
    // and collection2 less than 4
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(
            cloudClient.getZkStateReader(), collection2) < 4);

    assertUlogDir(collections);

    killAllJettys(jettys);
    killControlJetty();

    assertTrue("Timeout waiting for all not live", ClusterStateUtil
        .waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));

    Thread.sleep(2000);

    startAllJettys();

    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection1, 4, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection3, highShards, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection4, highShards, FULL_RELOAD_TIME);
    
    // all docs should be queried
    assertSingleReplicationAndShardSize(collection4, highShards);
    queryAndAssertResultSize(collection4, numDocs, 10000);

    assertSliceAndReplicaCount(collection1, 2);
    assertSingleReplicationAndShardSize(collection3, highShards);

    assertUlogDir(collections);

    int jettyIndex = random().nextInt(jettys.size());
    killJetty(jettyIndex);

    Thread.sleep(2000);

    startJetty(jettyIndex);

    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection1, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
        cloudClient.getZkStateReader(), collection3, highShards, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
        cloudClient.getZkStateReader(), collection4, highShards, FULL_RELOAD_TIME);

    assertSliceAndReplicaCount(collection1, 2);

    assertUlogDir(collections);

    assertSingleReplicationAndShardSize(collection3, highShards);


    assertSingleReplicationAndShardSize(collection4, highShards);

    Thread.sleep(2000);

    //disable autoAddReplicas
    Map m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS,
        "val", "false");

    SolrRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);

    int currentCount = ClusterStateUtil.getLiveAndActiveReplicaCount(
        cloudClient.getZkStateReader(), collection1);


    killJetty(3);

    // workLoopDelay=3s and waitAfterExpiration=2s
    Thread.sleep(15000);
    // Ensures that autoAddReplicas has not kicked in.
    assertTrue(currentCount > ClusterStateUtil.getLiveAndActiveReplicaCount(
        cloudClient.getZkStateReader(), collection1));


    Thread.sleep(2000);

    //enable autoAddReplicas
    m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS);

    request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);


    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection1, FULL_RELOAD_TIME);
    assertSliceAndReplicaCount(collection1, 2);

    assertUlogDir(collections);

    killJetty(random().nextInt(4));

    Thread.sleep(5000);

    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection1, 4, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection3, highShards, FULL_RELOAD_TIME);
    ClusterStateUtil.waitForReplicasUp(
            cloudClient.getZkStateReader(), collection4, highShards, FULL_RELOAD_TIME);

    // restart all to test core saved state

    List<JettySolrRunner> sJettys = new ArrayList<>(jettys);
    Collections.shuffle(sJettys, r);

    killAllJettys(sJettys);
    killControlJetty();

    assertTrue("Timeout waiting for all not live", ClusterStateUtil
        .waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));

    Collections.shuffle(sJettys, r);

    Thread.sleep(2000);

    ChaosMonkey.start(sJettys);
    ChaosMonkey.start(controlJetty);

    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection1, FULL_RELOAD_TIME);

    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection3, FULL_RELOAD_TIME);

    ClusterStateUtil.waitAllReplicasUp(cloudClient.getZkStateReader(), collection4, FULL_RELOAD_TIME);

    assertSliceAndReplicaCount(collection1, 2);


    assertSingleReplicationAndShardSize(collection3, highShards);

    // all docs should be queried
    assertSingleReplicationAndShardSize(collection4, highShards);
    queryAndAssertResultSize(collection4, numDocs, 10000);
    assertUlogDir(collections);
  }

  private void queryAndAssertResultSize(String collection, int expectedResultSize, int timeoutMS)
      throws SolrServerException, IOException, InterruptedException {
    long startTimestamp = System.nanoTime();

    long actualResultSize = 0;
    while(true) {
      if (System.nanoTime() - startTimestamp > TimeUnit.MILLISECONDS.toNanos(timeoutMS) || actualResultSize > expectedResultSize) {
        fail("expected: " + expectedResultSize + ", actual: " + actualResultSize);
      }
      SolrParams queryAll = new SolrQuery("*:*");
      cloudClient.setDefaultCollection(collection);
      QueryResponse queryResponse = cloudClient.query(queryAll);
      actualResultSize = queryResponse.getResults().getNumFound();
      if(expectedResultSize == actualResultSize) {
        return;
      }

      Thread.sleep(1000);
    }
  }

  private void addDocs(String collection, int numDocs, boolean commit) throws SolrServerException, IOException {
    for (int docId = 1; docId <= numDocs; docId++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docId);
      doc.addField("text", "shard" + docId % 5);
      cloudClient.setDefaultCollection(collection);
      cloudClient.add(doc);
    }
    if (commit) {
      cloudClient.commit();
    }
  }

  /**
   * After failover, ulogDir should not be changed.
   */
  private void assertUlogDir(String... collections) {
    for (String collection : collections) {
      Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getSlices();
      for (Slice slice : slices) {
        for (Replica replica : slice.getReplicas()) {
          Map<String, Object> properties = replica.getProperties();
          String coreName = replica.getCoreName();
          String curUlogDir = (String) properties.get(CoreDescriptor.CORE_ULOGDIR);
          String prevUlogDir = collectionUlogDirMap.get(coreName);
          if (curUlogDir != null) {
            if (prevUlogDir == null) {
              collectionUlogDirMap.put(coreName, curUlogDir);
            } else {
              assertEquals(prevUlogDir, curUlogDir);
            }
          }
        }
      }
    }
  }

  private void assertSingleReplicationAndShardSize(String collection, int numSlices) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getActiveSlices();
    assertEquals(numSlices, slices.size());
    for (Slice slice : slices) {
      assertEquals(1, slice.getReplicas().size());
    }
  }

  private void assertSliceAndReplicaCount(String collection, int sliceAndReplicaCount) {
    Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getActiveSlices();

    assertEquals(sliceAndReplicaCount, slices.size());
    for (Slice slice : slices) {
      assertEquals(sliceAndReplicaCount, slice.getReplicas().size());
    }
  }
  
  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
  }
  
  private void startJetty(int jettyIndex) throws Exception {
    log.info("Starting jetty: " + jettyIndex);
    ChaosMonkey.start(jettys.get(jettyIndex));
  }
  
  private void startAllJettys() throws Exception {
    log.info("Starting jetty: all");
    ChaosMonkey.start(jettys);
    ChaosMonkey.start(controlJetty);
  }

  private void killControlJetty() throws Exception {
    log.info("Killing jetty: control");
    ChaosMonkey.stop(controlJetty);
  }

  private void killAllJettys(List<JettySolrRunner> sJettys) throws Exception {
    log.info("Killing jetty: all");
    ChaosMonkey.stop(sJettys);
  }

  
  private void killJetty(int jettyIndex) throws Exception {
    log.info("Killing jetty: " + jettyIndex);
    ChaosMonkey.stop(jettys.get(jettyIndex));
  }
}
