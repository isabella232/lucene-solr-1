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
package org.apache.solr.core.backup;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.cloud.ShardRequestProcessor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * A concrete implementation of {@linkplain IndexBackupStrategy} which uses Solr core Admin
 * API to create a backup for each core associated with the specified collection.
 */
public class CopyFilesStrategy implements IndexBackupStrategy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ShardRequestProcessor processor;
  private final String backupRepoName;
  private final Optional<CollectionSnapshotMetaData> snapshotMeta;

  public CopyFilesStrategy(ShardRequestProcessor processor, String backupRepoName, Optional<CollectionSnapshotMetaData> snapshotMeta) {
    this.processor = processor;
    this.backupRepoName = backupRepoName;
    this.snapshotMeta = snapshotMeta;
  }

  @Override
  public void createBackup(URI basePath, String collectionName, String backupName) {
    ZkStateReader zkStateReader = processor.getZkStateReader();

    log.info("Starting backup of collection={} with backupName={} at location={}", collectionName, backupName,
        basePath);

    Collection<String> shardsToConsider = Collections.emptySet();
    if (snapshotMeta.isPresent()) {
      shardsToConsider = snapshotMeta.get().getShards();
    }

    for (Slice slice : zkStateReader.getClusterState().getCollection(collectionName).getActiveSlices()) {
      Replica replica = null;

      if (snapshotMeta.isPresent()) {
        if (!shardsToConsider.contains(slice.getName())) {
          log.warn("Skipping the backup for shard {} since it wasn't part of the collection {} when snapshot {} was created.",
              slice.getName(), collectionName, snapshotMeta.get().getName());
          continue;
        }
        replica = selectReplicaWithSnapshot(snapshotMeta.get(), slice);
      } else {
        // Note - Actually this can return a null value when there is no leader for this shard. This should be handled
        // gracefully.
        replica = slice.getLeader();
      }

      String coreName = replica.getStr(CORE_NAME_PROP);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.BACKUPCORE.toString());
      params.set(NAME, slice.getName());
      params.set(CoreAdminParams.BACKUP_REPOSITORY, backupRepoName);
      params.set(CoreAdminParams.BACKUP_LOCATION, basePath.getPath()); // note: index dir will be here then the "snapshot." + slice name
      params.set(CORE_NAME_PROP, coreName);
      if (snapshotMeta.isPresent()) {
        params.set(CoreAdminParams.COMMIT_NAME, snapshotMeta.get().getName());
      }

      processor.sendShardRequest(replica.getNodeName(), params);
      log.debug("Sent backup request to core={} for backupname={}", coreName, backupName);
    }

    log.info("Sent backup requests to all shard leaders for backupName={}", backupName);
    processor.processResponses( true, "Could not backup all replicas", Collections.<String>emptySet());
  }

  private Replica selectReplicaWithSnapshot(CollectionSnapshotMetaData snapshotMeta, Slice slice) {
    // The goal here is to choose the snapshot of the replica which was the leader at the time snapshot was created.
    // If that is not possible, we choose any other replica for the given shard.
    Collection<CoreSnapshotMetaData> snapshots = snapshotMeta.getReplicaSnapshotsForShard(slice.getName());

    Optional<CoreSnapshotMetaData> leaderCore = Optional.absent();
    for (CoreSnapshotMetaData m : snapshots) {
      if (m.isLeader()) {
        leaderCore = Optional.of(m);
        break;
      }
    }

    if (leaderCore.isPresent()) {
      log.info("Replica {} was the leader when snapshot {} was created.", leaderCore.get().getCoreName(), snapshotMeta.getName());
      Replica r = slice.getReplica(leaderCore.get().getCoreName());
      if ((r != null) && !r.getState().equals(ZkStateReader.DOWN)) {
        return r;
      }
    }

    for (Replica r : slice.getReplicas()) {
      if (!r.getState().equals(ZkStateReader.DOWN) && snapshotMeta.isSnapshotExists(slice.getName(), r)) {
        return r;
      }
    }

    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Unable to find any live replica with a snapshot named " + snapshotMeta.getName() + " for shard " + slice.getName());
  }
}
