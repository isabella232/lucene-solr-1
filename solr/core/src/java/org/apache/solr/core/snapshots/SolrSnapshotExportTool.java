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

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * This class provides utility functions required for "Snapshot Export" functionality.
 */
public class SolrSnapshotExportTool implements Closeable {
  private final CloudSolrServer solrClient;
  private final String collectionName;
  private final String snapshotName;

  public SolrSnapshotExportTool(String solrZkEnsemble, String collectionName, String snapshotName) {
    this.collectionName = collectionName;
    this.snapshotName = snapshotName;
    this.solrClient = new CloudSolrServer(solrZkEnsemble);
  }

  @Override
  public void close() throws IOException {
    if (this.solrClient != null) {
      this.solrClient.shutdown();
    }
  }

  public Map<String, List<String>> getIndexFilesPathForSnapshot() throws SolrServerException, IOException {
    Map<String, List<String>> result = new HashMap<>();

    Collection<CollectionSnapshotMetaData> snaps = listCollectionSnapshots(solrClient, collectionName);
    Optional<CollectionSnapshotMetaData> meta = Optional.absent();
    for (CollectionSnapshotMetaData m : snaps) {
      if (snapshotName.equals(m.getName())) {
        meta = Optional.of(m);
      }
    }

    if (!meta.isPresent()) {
      throw new IllegalArgumentException("The snapshot named " + snapshotName
          + " is not found for collection " + collectionName);
    }

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    for (Slice s : collectionState.getSlices()) {
      List<CoreSnapshotMetaData> replicaSnaps = meta.get().getReplicaSnapshotsForShard(s.getName());
      // Prepare a list of *existing* replicas (since one or more replicas could have been deleted after the snapshot creation).
      List<CoreSnapshotMetaData> availableReplicas = new ArrayList<>();
      for (CoreSnapshotMetaData m : replicaSnaps) {
        if (isReplicaAvailable(s, m.getCoreName())) {
          availableReplicas.add(m);
        }
      }

      if (availableReplicas.isEmpty()) {
        throw new IllegalArgumentException(
            "The snapshot named " + snapshotName + " not found for shard "
                + s.getName() + " of collection " + collectionName);
      }

      // Prefer a leader replica (at the time when the snapshot was created).
      CoreSnapshotMetaData coreSnap = availableReplicas.get(0);
      for (CoreSnapshotMetaData m : availableReplicas) {
        if (m.isLeader()) {
          coreSnap = m;
        }
      }

      List<String> paths = new ArrayList<>();
      for (String fileName : coreSnap.getFiles()) {
        paths.add(coreSnap.getIndexDirPath()+fileName);
      }

      result.put(s.getName(), paths);
    }

    return result;
  }

  public void buildCopyListings(String localFsPath) throws SolrServerException, IOException {
    Map<String, List<String>> paths = getIndexFilesPathForSnapshot();
    for (Map.Entry<String,List<String>> entry : paths.entrySet()) {
      StringBuilder filesBuilder = new StringBuilder();
      for (String filePath : entry.getValue()) {
        filesBuilder.append(filePath);
        filesBuilder.append("\n");
      }

      String files = filesBuilder.toString().trim();
      try (Writer w = new OutputStreamWriter(new FileOutputStream(new File(localFsPath, entry.getKey())), StandardCharsets.UTF_8)) {
        w.write(files);
      }
    }
  }

  public void backupCollectionMetaData(String backupLoc) throws SolrServerException, IOException {
    // Backup the collection meta-data
    CollectionAdminRequest.Backup backup = new CollectionAdminRequest.Backup(collectionName, snapshotName);
    backup.setIndexBackupStrategy(CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY);
    backup.setLocation(backupLoc);
    CollectionAdminResponse resp = backup.process(solrClient);
    Preconditions.checkState(resp.getStatus() == 0, "The request failed. The status code is " + resp.getStatus());
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      System.out.println("Usage: SolrSnapshotExportTool <solr_zk_ensemble> <collection_name> <snapshot_name> <local_output_dir> <hdfs_temp_dir>");
      System.exit(1);
    }

    String solrZkEnsemble = args[0];
    String collectionName = args[1];
    String snapshotName = args[2];
    String outputDir = args[3];
    String hdfsTempDir = args[4];

    try (SolrSnapshotExportTool tool = new SolrSnapshotExportTool(solrZkEnsemble, collectionName, snapshotName)) {
      tool.buildCopyListings(outputDir);
      System.out.println("Successfully prepared copylisting for the snapshot export.");
      tool.backupCollectionMetaData(hdfsTempDir);
      System.out.println("Successfully backed up collection meta-data");
      System.exit(0);
    } catch (Exception ex) {
      System.out.println("Unexpected error " + ex.getLocalizedMessage());
      System.exit(1);
    }
  }

  private static boolean isReplicaAvailable (Slice s, String coreName) {
    for (Replica r: s.getReplicas()) {
      if (coreName.equals(r.getCoreName())) {
        return true;
      }
    }
    return false;
  }

  private static Collection<CollectionSnapshotMetaData> listCollectionSnapshots(SolrServer adminClient, String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.ListSnapshots listSnapshots = new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(adminClient);

    Preconditions.checkState(resp.getStatus() == 0);

    NamedList apiResult = (NamedList) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<Object>)apiResult.getVal(i)));
    }

    return result;
  }
}
