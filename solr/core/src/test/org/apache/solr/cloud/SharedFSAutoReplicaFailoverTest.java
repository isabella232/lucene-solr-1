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

import static org.apache.solr.common.util.Utils.makeMap;

import java.io.IOException;
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

import javax.servlet.Filter;

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
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.junit.Ignore;

@Slow
@SuppressSSL
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
@Ignore
public class SharedFSAutoReplicaFailoverTest extends AbstractFullDistribZkTestBase {
  
  private static final boolean DEBUG = true;
  private static MiniDFSCluster dfsCluster;

  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  private final Map<String, String> collectionUlogDirMap = new HashMap<>();

  
  @BeforeClass
  public static void hdfsFailoverBeforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().getAbsolutePath());
    schemaString = "schema15.xml"; 
    System.setProperty("autoReplicaFailoverWorkLoopDelay", "3000");
    System.setProperty("autoReplicaFailoverWaitAfterExpiration", "2000");
  }
  
  @AfterClass
  public static void hdfsFailoverAfterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
    System.clearProperty("autoReplicaFailoverWorkLoopDelay");
    System.clearProperty("autoReplicaFailoverWaitAfterExpiration");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionUlogDirMap.clear();
    useJettyDataDir = false;
    System.setProperty("solr.xml.persist", "true");
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  public SharedFSAutoReplicaFailoverTest() {
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
    checkCreatedVsState = false;
    
  }
  
  @Override
  public void doTest() throws Exception {

  //  if (random().nextBoolean()) {
   //   CollectionsAPIDistributedZkTest.setClusterProp(getCommonCloudSolrServer(), "legacyCloud", "false");
  // } else {
      CollectionsAPIDistributedZkTest.setClusterProp(getCommonCloudSolrServer(), "legacyCloud", "true");
   // }
    
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
    
    class ExpireThread extends Thread {
      public void run() {
        while (true) {
          try {
            Thread.sleep(random().nextInt(20000));
          } catch (InterruptedException e) {
            return;
          }
          if (isInterrupted()) return;
          FilterHolder dfilter = jettys.get(random().nextInt(jettys.size()))
              .getDispatchFilter();
          if (dfilter != null) {
            Filter filter = dfilter.getFilter();
            if (filter != null) {
              CoreContainer cores = ((SolrDispatchFilter) filter).getCores();
              if (cores != null) {
                long sessionId = cores.getZkController().getZkClient()
                    .getSolrZooKeeper().getSessionId();
                zkServer.expire(sessionId);
              }
            }
          }
          if (isInterrupted()) return;
        }
      }
    };
    
    ExpireThread thread = null;
    try {
      
      String collection1 = "solrj_collection";
      CollectionAdminResponse response = CollectionAdminRequest
          .createCollection(collection1, 2, 2, 15, null, "conf1", "myOwnField",
              true, cloudClient);
      assertEquals(0, response.getStatus());
      assertTrue(response.isSuccess());
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection1, 2, 260000));
      
      String collection2 = "solrj_collection2";
      CollectionAdminResponse response2 = CollectionAdminRequest
          .createCollection(collection2, 2, 2, 15, null, "conf1", "myOwnField",
              false, cloudClient);
      assertEquals(0, response2.getStatus());
      assertTrue(response2.isSuccess());
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection2, 2, 260000));
      
      String collection3 = "solrj_collection3";
      CollectionAdminResponse response3 = CollectionAdminRequest
          .createCollection(collection3, highShards, 1, 15, null, "conf1",
              "myOwnField", true, cloudClient);
      assertEquals(0, response3.getStatus());
      assertTrue(response3.isSuccess());
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection3, highShards, 260000));
      
      // a collection has only 1 replica per a shard
      String collection4 = "solrj_collection4";
      Create createCollectionRequest = new Create();
      createCollectionRequest.setCollectionName(collection4);
      createCollectionRequest.setNumShards(highShards);
      createCollectionRequest.setReplicationFactor(1);
      createCollectionRequest.setMaxShardsPerNode(100);
      createCollectionRequest.setConfigName("conf1");
      createCollectionRequest.setRouterField("text");
      createCollectionRequest.setAutoAddReplicas(true);
      CollectionAdminResponse response4 = createCollectionRequest
          .process(getCommonCloudSolrServer());
      
      assertEquals(0, response4.getStatus());
      assertTrue(response4.isSuccess());
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection4, highShards, 260000));
      
      // all collections
      String[] collections = {collection1, collection2, collection3,
          collection4};
      
      // add some documents to collection4
      final int numDocs = 100;
      addDocs(collection4, numDocs, false); // indexed but not committed
      
      // no result because not committed yet
      queryAndAssertResultSize(collection4, 0, 10000);
      
      assertUlogDir(collections);
      
      
      cloudClient.setDefaultCollection(collection4);
      cloudClient.commit(); // to query all docs
      
      ChaosMonkey.stop(jettys.get(1));
      ChaosMonkey.stop(jettys.get(2));
      
      Thread.sleep(5000);
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection1, 260000));
      
      assertSliceAndReplicaCount(collection1);
      
      assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection1));
      assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection2) < 4);
      
      thread = new ExpireThread();
      thread.start();
      
      // there are 4 standard jetties and one control jetty and 2 nodes stopped
      ClusterStateUtil.waitForLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection3, 3, 30000);
      
      Thread.sleep(5000);
      
      ClusterStateUtil.waitForLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection4, highShards, 30000);
      
      // all docs should be queried after failover
      thread.interrupt();
      thread.join();
      
      Thread.sleep(2000);
      
      ClusterStateUtil.waitForLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection4, highShards, 30000);

      assertSingleReplicationAndShardSize(collection4, highShards);
      queryAndAssertResultSize(collection4, numDocs, 10000);
      
      thread = new ExpireThread();
      thread.start();
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection1, 260000));
      
      thread.interrupt();
      thread.join();
      
      Thread.sleep(2000);
      
      assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection1));
      // and collection2 less than 4
      assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection2) < 4);
      
      thread = new ExpireThread();
      thread.start();
      
      assertUlogDir(collections);
      
      ChaosMonkey.stop(jettys);
      ChaosMonkey.stop(controlJetty);
      
      assertTrue("Timeout waiting for all not live", ClusterStateUtil
          .waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));
      
      thread.interrupt();
      thread.join();
      
      Thread.sleep(2000);
      
      ChaosMonkey.start(jettys);
      ChaosMonkey.start(controlJetty);
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection1, 2, 120000));
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection3, highShards, 120000));
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection4, highShards, 120000));
      
      // all docs should be queried
      assertSingleReplicationAndShardSize(collection4, highShards);
      queryAndAssertResultSize(collection4, numDocs, 10000);
      
      thread = new ExpireThread();
      thread.start();
      
      assertSliceAndReplicaCount(collection1);
      assertSingleReplicationAndShardSize(collection3, highShards);
      
      assertUlogDir(collections);
      
      int jettyIndex = random().nextInt(jettys.size());
      ChaosMonkey.stop(jettys.get(jettyIndex));
      
      thread.interrupt();
      thread.join();
      
      Thread.sleep(2000);
      
      ChaosMonkey.start(jettys.get(jettyIndex));
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection1, 60000));
      ClusterStateUtil.waitForLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection3, highShards, 30000);
      ClusterStateUtil.waitForLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection4, highShards, 30000);
      
      thread = new ExpireThread();
      thread.start();
      
      assertSliceAndReplicaCount(collection1);
      
      assertUlogDir(collections);
      
      assertSingleReplicationAndShardSize(collection3, highShards);

      
      assertSingleReplicationAndShardSize(collection4, highShards);
      
      thread.interrupt();
      thread.join();
      Thread.sleep(2000);
      
      // disable autoAddReplicas
      Map m = makeMap("action",
          CollectionParams.CollectionAction.CLUSTERPROP.toLower(), "name",
          ZkStateReader.AUTO_ADD_REPLICAS, "val", "false");
      
      SolrRequest request = new QueryRequest(new MapSolrParams(m));
      request.setPath("/admin/collections");
      cloudClient.request(request);
      
      int currentCount = ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection1);
      
      thread = new ExpireThread();
      thread.start();
      
      ChaosMonkey.stop(jettys.get(3));
      
      // workLoopDelay=3s and waitAfterExpiration=2s
      Thread.sleep(15000);
      // Ensures that autoAddReplicas has not kicked in.
      assertTrue(currentCount > ClusterStateUtil.getLiveAndActiveReplicaCount(
          cloudClient.getZkStateReader(), collection1));
      
      
      thread.interrupt();
      thread.join();
      Thread.sleep(2000);
      
      // enable autoAddReplicas
      m = makeMap("action",
          CollectionParams.CollectionAction.CLUSTERPROP.toLower(), "name",
          ZkStateReader.AUTO_ADD_REPLICAS);
      
      request = new QueryRequest(new MapSolrParams(m));
      request.setPath("/admin/collections");
      cloudClient.request(request);
      
      thread = new ExpireThread();
      thread.start();
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection1, 120000));
      assertSliceAndReplicaCount(collection1);
      
      assertUlogDir(collections);
      
      ChaosMonkey.stop(jettys.get(random().nextInt(4)));
      
      Thread.sleep(5000);
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection1, 2, 120000));
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection3, highShards, 120000));
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForLiveAndActiveReplicaCount(
              cloudClient.getZkStateReader(), collection4, highShards,120000));
      
      // restart all to test core saved state
      
      List<JettySolrRunner> sJettys = new ArrayList<>(jettys);
      Collections.shuffle(sJettys);
      
      ChaosMonkey.stop(sJettys);
      ChaosMonkey.stop(controlJetty);
      
      assertTrue("Timeout waiting for all not live", ClusterStateUtil
          .waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));
      
      Collections.shuffle(sJettys);
      
      thread.interrupt();
      thread.join();
      Thread.sleep(2000);
      
      ChaosMonkey.start(sJettys);
      ChaosMonkey.start(controlJetty);
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection1, 120000));
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection3, 120000));
      
      assertTrue("Timeout waiting for all live and active",
          ClusterStateUtil.waitForAllActiveAndLiveReplicas(
              cloudClient.getZkStateReader(), collection4, 120000));
      
      thread = new ExpireThread();
      thread.start();
      
      assertSliceAndReplicaCount(collection1);

      
      assertSingleReplicationAndShardSize(collection3, highShards);
      
      // all docs should be queried
      assertSingleReplicationAndShardSize(collection4, highShards);
      queryAndAssertResultSize(collection4, numDocs, 10000);
      assertUlogDir(collections);
    } finally {
      if (thread != null) {
        thread.interrupt();
      }
    }
  }

  private void queryAndAssertResultSize(String collection, int expectedResultSize, int timeoutMS)
      throws SolrServerException, IOException, InterruptedException {
    long startTimestamp = System.nanoTime();

    long actualResultSize = 0;
    while(true) {
      if (System.nanoTime() - startTimestamp > TimeUnit.NANOSECONDS.convert(timeoutMS, TimeUnit.MILLISECONDS) || actualResultSize > expectedResultSize) {
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
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection);
    assertEquals(numSlices, slices.size());
    for (Slice slice : slices) {
      assertEquals(1, slice.getReplicas().size());
    }
  }
  
  private void assertSliceAndReplicaCount(String collection) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection);
    assertEquals(2, slices.size());
    for (Slice slice : slices) {
      assertEquals(2, slice.getReplicas().size());
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("solr.xml.persist");
  }
}
