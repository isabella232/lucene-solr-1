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
package org.apache.solr.core.snapshots;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.ListSnapshots;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.handler.BackupRestoreUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

@SolrTestCaseJ4.SuppressSSL // Currently unknown why SSL does not work with this test
@Slow
public class TestSolrCloudSnapshots extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static long docsSeed; // see indexDocs()
  private static final int NUM_SHARDS = 2;
  private static final int NUM_REPLICAS = 1;
  private static final int NUM_NODES = NUM_REPLICAS * NUM_SHARDS;

  @BeforeClass
  public static void setupClass() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    configureCluster(NUM_NODES)// nodes
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    docsSeed = random().nextLong();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
  }

  @Test
  public void testSnapshots() throws Exception {
    CloudSolrServer solrClient = cluster.getSolrClient();
    String collectionName = "SolrCloudSnapshots";
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setCollectionName(collectionName);
    create.setConfigName("conf1");
    create.setNumShards(NUM_SHARDS);
    create.setReplicationFactor(NUM_REPLICAS);
    assertEquals(0, create.process(solrClient).getStatus());
    solrClient.setDefaultCollection(collectionName);

    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);
    BackupRestoreUtils.verifyDocs(nDocs, solrClient, collectionName);

    String commitName = TestUtil.randomSimpleString(random(), 1, 5);

    int expectedCoresWithSnapshot = NUM_NODES;

    CollectionAdminRequest.CreateSnapshot createSnap = new CollectionAdminRequest.CreateSnapshot(collectionName, commitName);
    createSnap.process(solrClient);

    Collection<CollectionSnapshotMetaData> collectionSnaps = listCollectionSnapshots(solrClient, collectionName);
    assertEquals(1, collectionSnaps.size());
    CollectionSnapshotMetaData meta = collectionSnaps.iterator().next();
    assertEquals(commitName, meta.getName());
    assertEquals(CollectionSnapshotMetaData.SnapshotStatus.Successful, meta.getStatus());
    assertEquals(expectedCoresWithSnapshot, meta.getReplicaSnapshots().size());

    Map<String, CoreSnapshotMetaData> snapshotByCoreName = new HashMap<>();
    for (CoreSnapshotMetaData t : meta.getReplicaSnapshots()) {
      snapshotByCoreName.put(t.getCoreName(), t);
    }

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    for ( Slice shard : collectionState.getActiveSlices() ) {
      for (Replica replica : shard.getReplicas()) {
        String replicaBaseUrl = replica.getStr(BASE_URL_PROP);
        String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);

        assertTrue(snapshotByCoreName.containsKey(coreName));
        CoreSnapshotMetaData coreSnapshot = snapshotByCoreName.get(coreName);

        SolrServer adminClient = new HttpSolrServer(replicaBaseUrl);
        try {
          Collection<SnapshotMetaData> snapshots = listCoreSnapshots(adminClient, coreName);

          Optional<SnapshotMetaData> metaData = Optional.absent();
          for (SnapshotMetaData m : snapshots) {
            if (m.getName().equals(commitName)) {
              metaData = Optional.of(m);
              break;
            }
          }

          assertTrue("Snapshot not created for core " + coreName, metaData.isPresent());
          assertEquals(coreSnapshot.getIndexDirPath(), metaData.get().getIndexDirPath());
          assertEquals(coreSnapshot.getGenerationNumber(), metaData.get().getGenerationNumber());
        } finally {
          if (adminClient != null) {
            adminClient.shutdown();
          }
        }
      }
    }

    // Delete all documents.
    solrClient.deleteByQuery("*:*");
    solrClient.commit();
    BackupRestoreUtils.verifyDocs(0, solrClient, collectionName);

    String backupLocation = createTempDir().getAbsolutePath();
    String backupName = "mytestbackup";
    String restoreCollectionName = collectionName + "_restored";

    // Create a backup using the earlier created snapshot.
    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName);
      backup.setLocation(backupLocation);
      backup.setCommitName(commitName);
      assertEquals(0, backup.process(solrClient).getStatus());
    }

    // Verify if all the files listed are present in the backup.
    {
      collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
      for (Slice s : collectionState.getSlices()) {
        List<CoreSnapshotMetaData> replicaSnaps = meta.getReplicaSnapshotsForShard(s.getName());
        assertFalse(replicaSnaps.isEmpty());

        Path backupPath = Paths.get(backupLocation, backupName, "snapshot."+s.getName());
        CoreSnapshotMetaData coreSnap = replicaSnaps.get(0);
        Set<String> expectedFiles = new HashSet<>(coreSnap.getFiles());
        try (SimpleFSDirectory dir = new SimpleFSDirectory(backupPath.toFile())) {
          Set<String> actualFiles = new HashSet<String>(Arrays.asList(dir.listAll()));
          assertEquals(expectedFiles, actualFiles);
        }
      }
    }

    // Restore the backup
    {
      CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName);
      restore.setLocation(backupLocation);
      assertEquals(0, restore.process(solrClient).getStatus());

      AbstractDistribZkTestBase.waitForRecoveriesToFinish(
          restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);
      solrClient.setDefaultCollection(restoreCollectionName);


      CloudSolrServer svc = new CloudSolrServer(cluster.getZkServer().getZkAddress());
      try {
        svc.setDefaultCollection(restoreCollectionName);
        BackupRestoreUtils.verifyDocs(nDocs, svc, restoreCollectionName);
      } catch (Exception ex) {
        log.warn("Unexpected error", ex);
      } finally {
        svc.shutdown();
      }
    }

    // Delete snapshot
    CollectionAdminRequest.DeleteSnapshot deleteSnap = new CollectionAdminRequest.DeleteSnapshot(collectionName, commitName);
    deleteSnap.process(solrClient);

    // Wait for a while so that the clusterstate.json updates are propagated to the client side.
    Thread.sleep(2000);
    collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);

    for ( Slice shard : collectionState.getActiveSlices() ) {
      for (Replica replica : shard.getReplicas()) {
        String replicaBaseUrl = replica.getStr(BASE_URL_PROP);
        String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);

        SolrServer adminClient = new HttpSolrServer(replicaBaseUrl);
        try {
          Collection<SnapshotMetaData> snapshots = listCoreSnapshots(adminClient, coreName);
          Optional<SnapshotMetaData> metaData = Optional.absent();
          for (SnapshotMetaData m : snapshots) {
            if (m.getName().equals(commitName)) {
              metaData = Optional.of(m);
              break;
            }
          }

          assertFalse("Snapshot not deleted for core " + coreName, metaData.isPresent());
          // Remove the entry for core if the snapshot is deleted successfully.
          snapshotByCoreName.remove(coreName);
        } finally {
          if (adminClient != null) {
            adminClient.shutdown();
          }
        }
      }
    }

    // Verify all core-level snapshots are deleted.
    assertTrue("The cores remaining " + snapshotByCoreName, snapshotByCoreName.isEmpty());
    assertTrue(listCollectionSnapshots(solrClient, collectionName).isEmpty());
  }

  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(SolrServer adminClient, String collectionName) throws Exception {
    CollectionAdminRequest.ListSnapshots listSnapshots = new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(adminClient);

    assertTrue( resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO) instanceof NamedList );
    NamedList apiResult = (NamedList) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<Object>)apiResult.getVal(i)));
    }

    return result;
  }

  private Collection<SnapshotMetaData> listCoreSnapshots(SolrServer adminClient, String coreName) throws Exception {
    ListSnapshots req = new ListSnapshots();
    req.setCoreName(coreName);
    NamedList resp = adminClient.request(req);
    assertTrue( resp.get(SolrSnapshotManager.SNAPSHOTS_INFO) instanceof NamedList );
    NamedList apiResult = (NamedList) resp.get(SolrSnapshotManager.SNAPSHOTS_INFO);

    List<SnapshotMetaData> result = new ArrayList<>(apiResult.size());
    for(int i = 0 ; i < apiResult.size(); i++) {
      String commitName = apiResult.getName(i);
      String indexDirPath = (String)((NamedList)apiResult.get(commitName)).get(SolrSnapshotManager.INDEX_DIR_PATH);
      long genNumber = Long.valueOf((String)((NamedList)apiResult.get(commitName)).get(SolrSnapshotManager.GENERATION_NUM));
      result.add(new SnapshotMetaData(commitName, indexDirPath, genNumber));
    }
    return result;
  }
}