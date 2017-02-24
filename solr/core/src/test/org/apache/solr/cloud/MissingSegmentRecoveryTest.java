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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SuppressSSL(bugUrl = "SOLR-6987")
public class MissingSegmentRecoveryTest extends SolrCloudTestCase {
  final String collection = getClass().getSimpleName();
  
  Replica leader;
  Replica replica;

  @BeforeClass
  public static void setupCluster() throws Exception {
    String solrXml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML
        .replace("<str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>", "");
    Path baseDir = createTempDir().toPath();
    // System.setProperty("coreRootDirectory", baseDir.toString());

    configureCluster(2, baseDir)
        .addConfig("conf", configset("cloud-minimal"))
        .withSolrXml(solrXml)
        .configure();
    useFactory("solr.StandardDirectoryFactory");
  }

  @Before
  public void setup() throws SolrServerException, IOException {
    cluster.createCollection(collection, 1, 2, "conf", Collections.singletonMap("maxShardsPerNode", "1"));
    waitForState();
    cluster.getSolrClient().setDefaultCollection(collection);

    List<SolrInputDocument> docs = new ArrayList<>();
    
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      docs.add(doc);
    }

    cluster.getSolrClient().add(docs);
    cluster.getSolrClient().commit();
    
    Slice slice = cluster.getSolrClient().getZkStateReader().getClusterState().getSlice(collection, "shard1");
    leader = slice.getLeader();
    for (Replica r : slice.getReplicas()) {
      if (r != leader) replica = r;
    }
    
    assertNotNull(leader);
    assertNotNull(replica);
    
    cluster.getSolrClient().setDefaultCollection(collection);
  }
  
  @After
  public void teardown() throws Exception {
    System.clearProperty("CoreInitFailedAction");
    CollectionAdminRequest.deleteCollection(collection, cluster.getSolrClient());
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    System.clearProperty("coreRootDirectory");
    resetFactory();
  }

  @Test
  public void testLeaderRecovery() throws Exception {
    System.setProperty("CoreInitFailedAction", "fromleader");

    // Simulate failure by truncating the segment_* files
    for (File segment : getSegmentFiles(replica)) {
      truncate(segment);
    }

    // Might not need a sledge-hammer to reload the core
    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    jetty.stop();
    jetty.start();

    waitForState();
    
    QueryResponse resp = cluster.getSolrClient().query(new SolrQuery("*:*"));
    assertEquals(10, resp.getResults().getNumFound());
  }

  private File[] getSegmentFiles(Replica replica) {
    try (SolrCore core = cluster.getReplicaJetty(replica).getCoreContainer().getCore(replica.getCoreName())) {
      File indexDir = new File(core.getDataDir(), "index");
      return indexDir.listFiles(new FilenameFilter() {
        
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("segments_");
        }
      });
    }
  }
  
  private void truncate(File file) throws IOException {
    Files.write(file.toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
  }
  
  // Upstream has a much more robust way of tracking this using zookeeper watches, but we'll hack this in for now.
  private void waitForState() {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    DocCollection collectionState = null;
    
    long end = TimeUnit.NANOSECONDS.convert(DEFAULT_TIMEOUT, TimeUnit.SECONDS) + System.nanoTime();
    while (System.nanoTime() < end) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // ignore
      }
      
      collectionState = reader.getClusterState().getCollection(collection);
      
      int activeReplicas = 0;
      
      if (collectionState != null && collectionState.getSlices().size() == 1) {
        Slice slice = collectionState.getSlices().iterator().next();
        for (Replica replica : slice.getReplicas()) {
          if (ZkStateReader.ACTIVE.equals(replica.getState())) activeReplicas++;
        }
        if (activeReplicas == 2) return;
      }
    }
    
    fail("Timed out waiting for 1 shard and 2 replicas to come online. last state: " + collectionState);
  }
}
