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
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.handler.BackupRestoreUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A unit test to verify CDH-60700
 */
public class BackCompatRestoreTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SHARDS = 2;
  protected static final String ROUTER_TYPE = CompositeIdRouter.NAME;

  @BeforeClass
  public static void setupClass() throws Exception {
    Path baseDir = createTempDir().toPath();

    System.setProperty("coreRootDirectory", baseDir.toString());
    useFactory("solr.NRTCachingDirectoryFactory");
    configureCluster(NUM_SHARDS, baseDir)// nodes
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("coreRootDirectory");
  }

  public void testBackupRestore() throws Exception {
    String collectionName = "BackCompatRestoreTest";
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setCollectionName(collectionName);
    create.setConfigName("conf1");
    create.setNumShards(NUM_SHARDS);
    create.setRouterName(ROUTER_TYPE); // In SOLR4.4.x (< CDH5.2.0) only compositeIdRouter was supported.

    create.process(cluster.getSolrClient());

    int nDocs = 0;
    CloudSolrServer client = new CloudSolrServer(cluster.getZkServer().getZkAddress());
    try {
      client.setDefaultCollection(collectionName);
      nDocs = BackupRestoreUtils.indexDocs(client, collectionName, random().nextLong());
    } finally {
      client.shutdown();
    }

    testBackupAndRestore(collectionName, nDocs);
  }

  private void testBackupAndRestore(String collectionName, int nDocs) throws Exception {
    String backupName = "mytestbackup";

    CloudSolrServer client = cluster.getSolrClient();
    DocCollection backupCollection = client.getZkStateReader().getClusterState().getCollection(collectionName);

    String location = createTempDir().getAbsolutePath();

    log.info("Triggering Backup command");

    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName);
      backup.setLocation(location);
      assertEquals(0, backup.process(client).getStatus());
    }

    // Modify the backup contents for back-compat testing
    updateCollectionState(location, backupName, collectionName);

    log.info("Triggering Restore command");

    String restoreCollectionName = collectionName + "_restored";
    {
      CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName);
      restore.setLocation(location);

      assertEquals(0, restore.process(client).getStatus());
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(
          restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);

      Thread.sleep(5000); // sleep to allow ZK updates to propagate to the client.
    }

    //Check the number of results are the same
    CloudSolrServer solrClient = new CloudSolrServer(cluster.getZkServer().getZkAddress());
    try {
      solrClient.setDefaultCollection(restoreCollectionName);
      BackupRestoreUtils.verifyDocs(nDocs, solrClient, restoreCollectionName);
    } finally {
      solrClient.shutdown();
    }

    DocCollection restoreCollection = client.getZkStateReader().getClusterState().getCollection(restoreCollectionName);
    assertEquals(Integer.valueOf(1), restoreCollection.getReplicationFactor());
    assertEquals(1, restoreCollection.getMaxShardsPerNode());
    assertEquals(false, restoreCollection.getAutoAddReplicas());
    assertEquals(backupCollection.getActiveSlices().iterator().next().getReplicas().size(),
        restoreCollection.getActiveSlices().iterator().next().getReplicas().size());
    assertEquals(cluster.getSolrClient().getZkStateReader().readConfigName(collectionName),
        cluster.getSolrClient().getZkStateReader().readConfigName(restoreCollectionName));
  }

  //Artificially change the collection state to match the pre CDH5.2.0 state.
  @SuppressWarnings({"unchecked", "rawtypes", "resource"})
  private static void updateCollectionState (String backupLoc, String backupName,
      String collectionName) throws IOException {
    Map collState = null;
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    URI zkStateDir = repo.createURI(backupLoc, backupName, BackupManager.ZK_STATE_DIR);
    try (IndexInput is = repo.openInput(zkStateDir,
        BackupManager.COLLECTION_PROPS_FILE, IOContext.DEFAULT)) {
      byte[] arr = new byte[(int) is.length()];
      is.readBytes(arr, 0, (int) is.length());
      collState = (Map)Utils.fromJSON(arr);
    }

    assertTrue(collState.containsKey(collectionName));
    assertTrue(collState.get(collectionName) instanceof Map);
    Map collProps = (Map)collState.get(collectionName);
    collProps.remove(ZkStateReader.REPLICATION_FACTOR);
    collProps.remove(ZkStateReader.MAX_SHARDS_PER_NODE);
    collProps.remove(ZkStateReader.AUTO_ADD_REPLICAS);
    collProps.remove(DocCollection.DOC_ROUTER);
    collProps.put(DocCollection.DOC_ROUTER_OLD, ROUTER_TYPE);

    log.info("The collection state after solr 4.4.x changes --> {}", 
        new String(Utils.toJSON(collState), StandardCharsets.UTF_8));

    URI dest = repo.createURI(backupLoc, backupName, BackupManager.ZK_STATE_DIR,
        BackupManager.COLLECTION_PROPS_FILE);
    try (OutputStream collectionStateOs = repo.createOutput(dest)) {
      collectionStateOs.write(Utils.toJSON(collState));
    }
  }
}
