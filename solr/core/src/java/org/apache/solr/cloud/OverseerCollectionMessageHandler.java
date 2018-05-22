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

import static org.apache.solr.cloud.Assign.getNodesForNewShard;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RESTORE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESNAPSHOT;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Assign.Node;
import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.CopyFilesStrategy;
import org.apache.solr.core.backup.IndexBackupStrategy;
import org.apache.solr.core.backup.NoIndexBackupStrategy;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.SnapshotStatus;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.util.stats.Snapshot;
import org.apache.solr.util.stats.Timer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Optional;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related
 * overseer messages.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler {

  public static final String NUM_SLICES = "numShards";
  
  // @Deprecated- see on ZkStateReader
  public static final String REPLICATION_FACTOR = "replicationFactor";
  
  // @Deprecated- see on ZkStateReader
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";

  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";

  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;
  
  public static final String DELETECOLLECTION = "deletecollection";

  public static final String CREATECOLLECTION = "createcollection";

  public static final String RELOADCOLLECTION = "reloadcollection";
  
  public static final String CREATEALIAS = "createalias";
  
  public static final String DELETEALIAS = "deletealias";
  
  public static final String SPLITSHARD = "splitshard";

  public static final String DELETESHARD = "deleteshard";

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String ASYNC = "async";

  public static final String CREATESHARD = "createshard";

  public static final String DELETEREPLICA = "deletereplica";

  public static final String MIGRATE = "migrate";

  public static final String REQUESTID = "requestid";

  public static final String COLL_CONF = "collection.configName";

  public static final String COLL_PROP_PREFIX = "property.";

  public static final Map<String,Object> COLL_PROPS = ZkNodeProps.makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.MAX_SHARDS_PER_NODE, "1",
      ZkStateReader.AUTO_ADD_REPLICAS, "false");


  private static Logger log = LoggerFactory
      .getLogger(OverseerCollectionMessageHandler.class);

  private Overseer overseer;
  private ShardHandlerFactory shardHandlerFactory;
  private String adminPath;
  private ZkStateReader zkStateReader;
  private String myId;
  private Overseer.Stats stats;
  private OverseerNodePrioritizer overseerPrioritizer;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.
  final private Set collectionWip;

  public OverseerCollectionMessageHandler(ZkStateReader zkStateReader, String myId,
                                        final ShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Overseer.Stats stats,
                                        Overseer overseer,
                                        OverseerNodePrioritizer overseerPrioritizer) {
    this.zkStateReader = zkStateReader;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    this.overseerPrioritizer = overseerPrioritizer;
    this.collectionWip = new HashSet();
  }

  @Override
  public SolrResponse processMessage(ZkNodeProps message, String operation) {
    log.warn("OverseerCollectionMessageHandler.processMessage : "+ operation + " , "+ message.toString());

    NamedList results = new NamedList();
    try {
      if (CREATECOLLECTION.equals(operation)) {
        createCollection(zkStateReader.getClusterState(), message, results);
      } else if (DELETECOLLECTION.equals(operation)) {
        deleteCollection(message, results);
      } else if (RELOADCOLLECTION.equals(operation)) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());
        collectionCmd(zkStateReader.getClusterState(), message, params, results, ZkStateReader.ACTIVE);
      } else if (CREATEALIAS.equals(operation)) {
        createAlias(zkStateReader.getAliases(), message);
      } else if (DELETEALIAS.equals(operation)) {
        deleteAlias(zkStateReader.getAliases(), message);
      } else if (SPLITSHARD.equals(operation))  {
        splitShard(zkStateReader.getClusterState(), message, results);
      } else if (CREATESHARD.equals(operation))  {
        createShard(zkStateReader.getClusterState(), message, results);
      } else if (DELETESHARD.equals(operation)) {
        deleteShard(zkStateReader.getClusterState(), message, results);
      } else if (DELETEREPLICA.equals(operation)) {
        deleteReplica(zkStateReader.getClusterState(), message, results);
      } else if (MIGRATE.equals(operation)) {
        migrate(zkStateReader.getClusterState(), message, results);
      } else if(REMOVEROLE.isEqual(operation) || ADDROLE.isEqual(operation) ){
        processRoleCommand(message, operation);
      } else if (ADDREPLICA.isEqual(operation))  {
        addReplica(zkStateReader.getClusterState(), message, results);
      } else if (OVERSEERSTATUS.isEqual(operation)) {
        getOverseerStatus(message, results);
      } else if (CLUSTERSTATUS.isEqual(operation)) {
        getClusterStatus(zkStateReader.getClusterState(), message, results);
      } else if (BACKUP.isEqual(operation)) {
        processBackupAction(message, results);
      } else if (RESTORE.isEqual(operation)) {
        processRestoreAction(message, results);
      } else if (CREATESNAPSHOT.isEqual(operation)) {
        processCreateSnapshotAction(message, results);
      } else if (DELETESNAPSHOT.isEqual(operation)) {
        processDeleteSnapshotAction(message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }
    } catch (Exception e) {
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr("name");

      if (collName == null) {
        SolrException.log(log, "Operation " + operation + " failed", e);
      } else  {
        SolrException.log(log, "Collection: " + collName + " operation: " + operation
            + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  private void getOverseerStatus(ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String leaderNode = OverseerTaskProcessor.getLeaderNode(zkStateReader.getZkClient());
    results.add("leader", leaderNode);
    Stat stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue",null, stat, true);
    results.add("overseer_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue-work",null, stat, true);
    results.add("overseer_work_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/collection-queue-work",null, stat, true);
    results.add("overseer_collection_queue_size", stat.getNumChildren());

    NamedList overseerStats = new NamedList();
    NamedList collectionStats = new NamedList();
    NamedList stateUpdateQueueStats = new NamedList();
    NamedList workQueueStats = new NamedList();
    NamedList collectionQueueStats = new NamedList();
    for (Map.Entry<String, Overseer.Stat> entry : stats.getStats().entrySet()) {
      String key = entry.getKey();
      NamedList<Object> lst = new SimpleOrderedMap<>();
      if (key.startsWith("collection_"))  {
        collectionStats.add(key.substring(11), lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
        List<Overseer.FailedOp> failureDetails = stats.getFailureDetails(key);
        if (failureDetails != null) {
          List<SimpleOrderedMap<Object>> failures = new ArrayList<>();
          for (Overseer.FailedOp failedOp : failureDetails) {
            SimpleOrderedMap<Object> fail = new SimpleOrderedMap<>();
            fail.add("request", failedOp.req.getProperties());
            fail.add("response", failedOp.resp.getResponse());
            failures.add(fail);
          }
          lst.add("recent_failures", failures);
        }
      } else if (key.startsWith("/overseer/queue_"))  {
        stateUpdateQueueStats.add(key.substring(16), lst);
      } else if (key.startsWith("/overseer/queue-work_"))  {
        workQueueStats.add(key.substring(21), lst);
      } else if (key.startsWith("/overseer/collection-queue-work_"))  {
        collectionQueueStats.add(key.substring(32), lst);
      } else  {
        // overseer stats
        overseerStats.add(key, lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
      }
      Timer timer = entry.getValue().requestTime;
      Snapshot snapshot = timer.getSnapshot();
      lst.add("totalTime", timer.getSum());
      lst.add("avgRequestsPerMinute", timer.getMeanRate());
      lst.add("5minRateRequestsPerMinute", timer.getFiveMinuteRate());
      lst.add("15minRateRequestsPerMinute", timer.getFifteenMinuteRate());
      lst.add("avgTimePerRequest", timer.getMean());
      lst.add("medianRequestTime", snapshot.getMedian());
      lst.add("75thPctlRequestTime", snapshot.get75thPercentile());
      lst.add("95thPctlRequestTime", snapshot.get95thPercentile());
      lst.add("99thPctlRequestTime", snapshot.get99thPercentile());
      lst.add("999thPctlRequestTime", snapshot.get999thPercentile());
    }
    results.add("overseer_operations", overseerStats);
    results.add("collection_operations", collectionStats);
    results.add("overseer_queue", stateUpdateQueueStats);
    results.add("overseer_internal_queue", workQueueStats);
    results.add("collection_queue", collectionQueueStats);

  }

  private void getClusterStatus(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

    // read aliases
    Aliases aliases = zkStateReader.getAliases();
    Map<String, List<String>> collectionVsAliases = new HashMap<>();
    Map<String, String> aliasVsCollections = aliases.getCollectionAliasMap();
    if (aliasVsCollections != null) {
      for (Map.Entry<String, String> entry : aliasVsCollections.entrySet()) {
        List<String> colls = StrUtils.splitSmart(entry.getValue(), ',');
        String alias = entry.getKey();
        for (String coll : colls) {
          if (collection == null || collection.equals(coll))  {
            List<String> list = collectionVsAliases.get(coll);
            if (list == null) {
              list = new ArrayList<>();
              collectionVsAliases.put(coll, list);
            }
            list.add(alias);
          }
        }
      }
    }

    Map roles = null;
    if (zkStateReader.getZkClient().exists(ZkStateReader.ROLES, true)) {
      roles = (Map) ZkStateReader.fromJSON(zkStateReader.getZkClient().getData(ZkStateReader.ROLES, null, null, true));
    }

    // convert cluster state into a map of writable types
    byte[] bytes = ZkStateReader.toJSON(clusterState);
    Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader.fromJSON(bytes);

    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
    NamedList<Object> collectionProps = new SimpleOrderedMap<Object>();
    if (collection == null) {
      Set<String> collections = clusterState.getCollections();
      for (String name : collections) {
        Map<String, Object> collectionStatus = getCollectionStatus(stateMap, name, shard);
        if (collectionVsAliases.containsKey(name) && !collectionVsAliases.get(name).isEmpty())  {
          collectionStatus.put("aliases", collectionVsAliases.get(name));
        }
        collectionProps.add(name, collectionStatus);
      }
    } else {
      String routeKey = message.getStr(ShardParams._ROUTE_);
      if (routeKey == null) {
        Map<String, Object> collectionStatus = getCollectionStatus(stateMap, collection, shard);
        if (collectionVsAliases.containsKey(collection) && !collectionVsAliases.get(collection).isEmpty())  {
          collectionStatus.put("aliases", collectionVsAliases.get(collection));
        }
        collectionProps.add(collection, collectionStatus);
      } else {
        DocCollection docCollection = clusterState.getCollection(collection);
        DocRouter router = docCollection.getRouter();
        Collection<Slice> slices = router.getSearchSlices(routeKey, null, docCollection);
        String s = "";
        for (Slice slice : slices) {
          s += slice.getName() + ",";
        }
        if (shard != null)  {
          s += shard;
        }
        Map<String, Object> collectionStatus = getCollectionStatus(stateMap, collection, s);
        if (collectionVsAliases.containsKey(collection) && !collectionVsAliases.get(collection).isEmpty())  {
          collectionStatus.put("aliases", collectionVsAliases.get(collection));
        }
        collectionProps.add(collection, collectionStatus);
      }
    }


    NamedList<Object> clusterStatus = new SimpleOrderedMap<>();
    clusterStatus.add("collections", collectionProps);

    // read cluster properties
    Map clusterProps = zkStateReader.getClusterProps();
    if (clusterProps != null && !clusterProps.isEmpty())  {
      clusterStatus.add("properties", clusterProps);
    }

    // add the alias map too
    if (aliasVsCollections != null && !aliasVsCollections.isEmpty())  {
      clusterStatus.add("aliases", aliasVsCollections);
    }

    // add the roles map
    if (roles != null)  {
      clusterStatus.add("roles", roles);
    }

    // add live_nodes
    List<String> liveNodes = zkStateReader.getZkClient().getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true);
    clusterStatus.add("live_nodes", liveNodes);

    results.add("cluster", clusterStatus);
  }

  /**
   * Get collection status from cluster state.
   * Can return collection status by given shard name.
   *
   *
   * @param clusterState cloud state map parsed from JSON-serialized {@link ClusterState}
   * @param name  collection name
   * @param shardStr comma separated shard names
   * @return map of collection properties
   */
  private Map<String, Object> getCollectionStatus(Map<String, Object> clusterState, String name, String shardStr) {
    Map<String, Object> docCollection = (Map<String, Object>) clusterState.get(name);
    if (docCollection == null)  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
    }
    if (shardStr == null) {
      return docCollection;
    } else {
      Map<String, Object> shards = (Map<String, Object>) docCollection.get("shards");
      Map<String, Object>  selected = new HashMap<>();
      List<String> selectedShards = Arrays.asList(shardStr.split(","));
      for (String selectedShard : selectedShards) {
        if (!shards.containsKey(selectedShard)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " shard: " + selectedShard + " not found");
        }
        selected.put(selectedShard, shards.get(selectedShard));
        docCollection.put("shards", selected);
      }
      return docCollection;
    }
  }

  private void processRoleCommand(ZkNodeProps message, String operation) throws KeeperException, InterruptedException {
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map roles = null;
    String node = message.getStr("node");

    String roleName = message.getStr("role");
    boolean nodeExists = false;
    if(nodeExists = zkClient.exists(ZkStateReader.ROLES, true)){
      roles = (Map) ZkStateReader.fromJSON(zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true));
    } else {
      roles = new LinkedHashMap(1);
    }

    List nodeList= (List) roles.get(roleName);
    if(nodeList == null) roles.put(roleName, nodeList = new ArrayList());
    if(ADDROLE.toString().toLowerCase(Locale.ROOT).equals(operation) ){
      log.info("Overseer role added to {}", node);
      if(!nodeList.contains(node)) nodeList.add(node);
    } else if(REMOVEROLE.toString().toLowerCase(Locale.ROOT).equals(operation)) {
      log.info("Overseer role removed from {}", node);
      nodeList.remove(node);
    }

    if(nodeExists){
      zkClient.setData(ZkStateReader.ROLES, ZkStateReader.toJSON(roles),true);
    } else {
      zkClient.create(ZkStateReader.ROLES, ZkStateReader.toJSON(roles), CreateMode.PERSISTENT,true);
    }
    //if there are too many nodes this command may time out. And most likely dedicated
    // overseers are created when there are too many nodes  . So , do this operation in a separate thread
    new Thread(){
      @Override
      public void run() {
        try {
          overseerPrioritizer.prioritizeOverseerNodes(myId);
        } catch (Exception e) {
          log.error("Error in prioritizing Overseer",e);
        }

      }
    }.start();
  }

  private void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);
      DocCollection coll = clusterState.getCollection(collectionName);
      Slice slice = coll.getSlice(shard);
      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
      if (slice == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid shard name : " + shard + " in collection : " + collectionName);
      }
      Replica replica = slice.getReplica(replicaName);
      if (replica == null) {
        ArrayList<String> l = new ArrayList<>();
        for (Replica r : slice.getReplicas()) l.add(r.getName());
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid replica : " + replicaName + " in shard/collection : "
            + shard + "/" + collectionName + " available replicas are " + StrUtils.join(l, ','));
      }

      String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
      String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

      // assume the core exists and try to unload it
      Map m = ZkNodeProps.makeMap("qt", adminPath, CoreAdminParams.ACTION,
          CoreAdminAction.UNLOAD.toString(), CoreAdminParams.CORE, core,
          CoreAdminParams.DELETE_INSTANCE_DIR, "true",
          CoreAdminParams.DELETE_DATA_DIR, "true");

      ShardRequest sreq = new ShardRequest();
      sreq.purpose = 1;
      sreq.shards = new String[]{baseUrl};
      sreq.actualShards = sreq.shards;
      sreq.params = new ModifiableSolrParams(new MapSolrParams(m));
      try {
        shardHandler.submit(sreq, baseUrl, sreq.params);
      } catch (Exception e) {
        log.warn("Exception trying to unload core " + sreq, e);
      }

      collectShardResponses(!Slice.ACTIVE.equals(replica.getStr(Slice.STATE)) ? new NamedList() : results,
          false, null, shardHandler);

      if (waitForCoreNodeGone(collectionName, shard, replicaName, 5000))
        return;//check if the core unload removed the corenode zk enry
      deleteCoreNode(collectionName, replicaName, replica, core); // try and ensure core info is removed from clusterstate
      if (waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return;

      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not  remove replica : " + collectionName + "/" + shard + "/" + replicaName);
  }

  private boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    boolean deleted = false;
    while (System.nanoTime() < waitUntil) {
      Thread.sleep(100);
      DocCollection docCollection = zkStateReader.getClusterState().getCollection(collectionName);
      if(docCollection != null) {
        Slice slice = docCollection.getSlice(shard);
        if(slice == null || slice.getReplica(replicaName) == null) {
          deleted =  true;
        }
      }
      // Return true if either someone already deleted the collection/slice/replica.
      if (docCollection == null || deleted) break;
    }
    return deleted;
  }

  private void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws KeeperException, InterruptedException {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, Overseer.DELETECORE,
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName, ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(replica.getStr(ZkStateReader.NODE_NAME_PROP)));
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));
  }

  private void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }
  }

  private void deleteCollection(ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collection = message.getStr("name");
    try {
      // Remove the snapshots meta-data for this collection in ZK. Deleting actual index files
      // should be taken care of as part of collection delete operation.
      SolrZkClient zkClient = this.overseer.getZkController().getZkClient();
      SolrSnapshotManager.cleanupCollectionLevelSnapshots(zkClient, collection);

      if (zkStateReader.getClusterState().getCollectionOrNull(collection) == null) {
        if (zkStateReader.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
          // if the collection is not in the clusterstate, but is listed in zk, do nothing, it will just
          // be removed in the finally - we cannot continue, because the below code will error if the collection
          // is not in the clusterstate
          return;
        }
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);
      
      Set<String> okayExceptions = new HashSet<>(1);
      okayExceptions.add(NonExistentCoreException.class.getName());
      
      collectionCmd(zkStateReader.getClusterState(), message, params, results, null, okayExceptions);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, Overseer.REMOVECOLLECTION, NAME, collection);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));

      // wait for a while until we don't see the collection
      long now = System.nanoTime();
      long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (System.nanoTime() < timeout) {
        Thread.sleep(100);
        removed = !zkStateReader.getClusterState().hasCollection(message.getStr(collection));
        if (removed) {
          Thread.sleep(300); // just a bit of time so it's more likely other
                             // readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + message.getStr("name"));
      }
      
    } finally {
      
      try {
        if (zkStateReader.getZkClient().exists(
            ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
          zkStateReader.getZkClient().clean(
              ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection);
        }
        log.info("Successfully deleted collection {}", collection);
      } catch (InterruptedException e) {
        SolrException.log(log, "Cleaning up collection in zk was interrupted:"
            + collection, e);
        Thread.currentThread().interrupt();
      } catch (KeeperException e) {
        SolrException.log(log, "Problem cleaning up collection in zk:"
            + collection, e);
      }
    }
  }

  private void createAlias(Aliases aliases, ZkNodeProps message) {
    String aliasName = message.getStr("name");
    String collections = message.getStr("collections");
    
    Map<String,Map<String,String>> newAliasesMap = new HashMap<>();
    Map<String,String> newCollectionAliasesMap = new HashMap<>();
    Map<String,String> prevColAliases = aliases.getCollectionAliasMap();
    if (prevColAliases != null) {
      newCollectionAliasesMap.putAll(prevColAliases);
    }
    newCollectionAliasesMap.put(aliasName, collections);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes = ZkStateReader.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES, jsonBytes, true);
      
      checkForAlias(aliasName, collections);
      // some fudge for other nodes
      Thread.sleep(100);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      log.warn("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }
  
  private void checkForAlias(String name, String value) {

    long now = System.nanoTime();
    long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases = null;
    while (System.nanoTime() < timeout) {
      aliases = zkStateReader.getAliases();
      String collections = aliases.getCollectionAlias(name);
      if (collections != null && collections.equals(value)) {
        success = true;
        break;
      }
    }
    if (!success) {
      log.warn("Timeout waiting to be notified of Alias change...");
    }
  }
  
  private void checkForAliasAbsence(String name) {

    long now = System.nanoTime();
    long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases = null;
    while (System.nanoTime() < timeout) {
      aliases = zkStateReader.getAliases();
      String collections = aliases.getCollectionAlias(name);
      if (collections == null) {
        success = true;
        break;
      }
    }
    if (!success) {
      log.warn("Timeout waiting to be notified of Alias change...");
    }
  }

  private void deleteAlias(Aliases aliases, ZkNodeProps message) {
    String aliasName = message.getStr("name");

    Map<String,Map<String,String>> newAliasesMap = new HashMap<>();
    Map<String,String> newCollectionAliasesMap = new HashMap<>();
    newCollectionAliasesMap.putAll(aliases.getCollectionAliasMap());
    newCollectionAliasesMap.remove(aliasName);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes  = ZkStateReader.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES,
          jsonBytes, true);
      checkForAliasAbsence(aliasName);
      // some fudge for other nodes
      Thread.sleep(100);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      log.warn("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } 
    
  }

  private boolean createShard(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);

      log.info("Create shard invoked: {}", message);
      if (collectionName == null || shard == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");
      int numSlices = 1;
      
      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
      DocCollection collection = clusterState.getCollection(collectionName);
      int maxShardsPerNode = collection.getInt(ZkStateReader.MAX_SHARDS_PER_NODE, 1);
      int repFactor = message.getInt(ZkStateReader.REPLICATION_FACTOR, collection.getInt(ZkStateReader.REPLICATION_FACTOR, 1));
      String createNodeSetStr = message.getStr(CREATE_NODE_SET);

      ArrayList<Node> sortedNodeList = getNodesForNewShard(clusterState, collectionName, numSlices, maxShardsPerNode, repFactor, createNodeSetStr);

      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));
      // wait for a while until we see the shard
      long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean created = false;
      while (System.nanoTime() < waitUntil) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().getCollection(collectionName).getSlice(shard) != null;
        if (created) break;
      }
      if (!created)
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create shard: " + message.getStr("name"));


      String configName = message.getStr(COLL_CONF);
    String sliceName = shard;
    for (int j = 1; j <= repFactor; j++) {
      String nodeName = sortedNodeList.get(((j - 1)) % sortedNodeList.size()).nodeName;
      String shardName = collectionName + "_" + sliceName + "_replica" + j;
      log.info("Creating shard " + shardName + " as part of slice "
          + sliceName + " of collection " + collectionName + " on "
          + nodeName);

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, shardName);
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, sliceName);
        params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);
        addPropertyParams(message, params);

        ShardRequest sreq = new ShardRequest();
        params.set("qt", adminPath);
        sreq.purpose = 1;
        String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
        sreq.shards = new String[]{replica};
        sreq.actualShards = sreq.shards;
        sreq.params = params;

        shardHandler.submit(sreq, replica, sreq.params);

      }

    processResponses(results, shardHandler, Collections.<String>emptySet());

      log.info("Finished create command on all shards for collection: "
          + collectionName);

      return true;
     
  }


  private boolean splitShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    String collectionName = message.getStr("collection");
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);
      log.info("Split shard invoked");
      String splitKey = message.getStr("split.key");
      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

      DocCollection collection = clusterState.getCollection(collectionName);
      DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;

      Slice parentSlice = null;

      if (slice == null) {
        if (router instanceof CompositeIdRouter) {
          Collection<Slice> searchSlices = router.getSearchSlicesSingle(splitKey, new ModifiableSolrParams(), collection);
          if (searchSlices.isEmpty()) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to find an active shard for split.key: " + splitKey);
          }
          if (searchSlices.size() > 1) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "Splitting a split.key: " + splitKey + " which spans multiple shards is not supported");
          }
          parentSlice = searchSlices.iterator().next();
          slice = parentSlice.getName();
          log.info("Split by route.key: {}, parent shard is: {} ", splitKey, slice);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Split by route key can only be used with CompositeIdRouter or subclass. Found router: " + router.getClass().getName());
        }
      } else {
        parentSlice = clusterState.getSlice(collectionName, slice);
      }

      if (parentSlice == null) {
        if (clusterState.hasCollection(collectionName)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collectionName);
        }
      }

      // find the leader for the shard
      Replica parentShardLeader = null;
      try {
        parentShardLeader = zkStateReader.getLeaderRetry(collectionName, slice, 10000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      DocRouter.Range range = parentSlice.getRange();
      if (range == null) {
        range = new PlainIdRouter().fullRange();
      }

      List<DocRouter.Range> subRanges = null;
      String rangesStr = message.getStr(CoreAdminParams.RANGES);
      if (rangesStr != null) {
        String[] ranges = rangesStr.split(",");
        if (ranges.length == 0 || ranges.length == 1) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "There must be at least two ranges specified to split a shard");
        } else {
          subRanges = new ArrayList<>(ranges.length);
          for (int i = 0; i < ranges.length; i++) {
            String r = ranges[i];
            try {
              subRanges.add(DocRouter.DEFAULT.fromString(r));
            } catch (Exception e) {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Exception in parsing hexadecimal hash range: " + r, e);
            }
            if (!subRanges.get(i).isSubsetOf(range)) {
              throw new SolrException(ErrorCode.BAD_REQUEST,
                  "Specified hash range: " + r + " is not a subset of parent shard's range: " + range.toString());
            }
          }
          List<DocRouter.Range> temp = new ArrayList<>(subRanges); // copy to preserve original order
          Collections.sort(temp);
          if (!range.equals(new DocRouter.Range(temp.get(0).min, temp.get(temp.size() - 1).max))) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "Specified hash ranges: " + rangesStr + " do not cover the entire range of parent shard: " + range);
          }
          for (int i = 1; i < temp.size(); i++) {
            if (temp.get(i - 1).max + 1 != temp.get(i).min) {
              throw new SolrException(ErrorCode.BAD_REQUEST,
                  "Specified hash ranges: " + rangesStr + " either overlap with each other or " +
                      "do not cover the entire range of parent shard: " + range);
            }
          }
        }
      } else if (splitKey != null) {
        if (router instanceof CompositeIdRouter) {
          CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
          subRanges = compositeIdRouter.partitionRangeByKey(splitKey, range);
          if (subRanges.size() == 1) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "The split.key: " + splitKey + " has a hash range that is exactly equal to hash range of shard: " + slice);
          }
          for (DocRouter.Range subRange : subRanges) {
            if (subRange.min == subRange.max) {
              throw new SolrException(ErrorCode.BAD_REQUEST, "The split.key: " + splitKey + " must be a compositeId");
            }
          }
          log.info("Partitioning parent shard " + slice + " range: " + parentSlice.getRange() + " yields: " + subRanges);
          rangesStr = "";
          for (int i = 0; i < subRanges.size(); i++) {
            DocRouter.Range subRange = subRanges.get(i);
            rangesStr += subRange.toString();
            if (i < subRanges.size() - 1)
              rangesStr += ',';
          }
        }
      } else {
        // todo: fixed to two partitions?
        subRanges = router.partitionRange(2, range);
      }

      try {
        List<String> subSlices = new ArrayList<>(subRanges.size());
        List<String> subShardNames = new ArrayList<>(subRanges.size());
        String nodeName = parentShardLeader.getNodeName();
        for (int i = 0; i < subRanges.size(); i++) {
          String subSlice = slice + "_" + i;
          subSlices.add(subSlice);
          String subShardName = collectionName + "_" + subSlice + "_replica1";
          subShardNames.add(subShardName);

          Slice oSlice = clusterState.getSlice(collectionName, subSlice);
          if (oSlice != null) {
            if (Slice.ACTIVE.equals(oSlice.getState())) {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Sub-shard: " + subSlice + " exists in active state. Aborting split shard.");
            } else if (Slice.CONSTRUCTION.equals(oSlice.getState()) || Slice.RECOVERY.equals(oSlice.getState())) {
              // delete the shards
              for (String sub : subSlices) {
                log.info("Sub-shard: {} already exists therefore requesting its deletion", sub);
                Map<String, Object> propMap = new HashMap<>();
                propMap.put(Overseer.QUEUE_OPERATION, "deleteshard");
                propMap.put(COLLECTION_PROP, collectionName);
                propMap.put(SHARD_ID_PROP, sub);
                ZkNodeProps m = new ZkNodeProps(propMap);
                try {
                  deleteShard(clusterState, m, new NamedList());
                } catch (Exception e) {
                  throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to delete already existing sub shard: " + sub, e);
                }
              }
            }
          }
        }

        // do not abort splitshard if the unloading fails
        // this can happen because the replicas created previously may be down
        // the only side effect of this is that the sub shard may end up having more replicas than we want
        collectShardResponses(results, false, null, shardHandler);

        String asyncId = message.getStr(ASYNC);
        HashMap<String, String> requestMap = new HashMap<String, String>();

        for (int i = 0; i < subRanges.size(); i++) {
          String subSlice = subSlices.get(i);
          String subShardName = subShardNames.get(i);
          DocRouter.Range subRange = subRanges.get(i);

          log.info("Creating slice "
              + subSlice + " of collection " + collectionName + " on "
              + nodeName);

        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, "createshard");
        propMap.put(ZkStateReader.SHARD_ID_PROP, subSlice);
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        propMap.put(ZkStateReader.SHARD_RANGE_PROP, subRange.toString());
        propMap.put(ZkStateReader.SHARD_STATE_PROP, Slice.CONSTRUCTION);
        propMap.put(ZkStateReader.SHARD_PARENT_PROP, parentSlice.getName());
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        inQueue.offer(ZkStateReader.toJSON(new ZkNodeProps(propMap)));

          // wait until we are able to see the new shard in cluster state
          waitForNewShard(collectionName, subSlice);

          // refresh cluster state
          clusterState = zkStateReader.getClusterState();

          log.info("Adding replica " + subShardName + " as part of slice "
              + subSlice + " of collection " + collectionName + " on "
              + nodeName);
          propMap = new HashMap<>();
          propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
          propMap.put(COLLECTION_PROP, collectionName);
          propMap.put(SHARD_ID_PROP, subSlice);
          propMap.put("node", nodeName);
          propMap.put(CoreAdminParams.NAME, subShardName);
          // copy over property params:
          for (String key : message.keySet()) {
            if (key.startsWith(COLL_PROP_PREFIX)) {
              propMap.put(key, message.getStr(key));
            }
          }
          // add async param
          if(asyncId != null) {
            propMap.put(ASYNC, asyncId);
          }
          addReplica(clusterState, new ZkNodeProps(propMap), results);
        }

        collectShardResponses(results, true,
            "SPLITSHARD failed to create subshard leaders", shardHandler);

        completeAsyncRequest(asyncId, requestMap, results);

        for (String subShardName : subShardNames) {
          // wait for parent leader to acknowledge the sub-shard core
          log.info("Asking parent leader to wait for: " + subShardName + " to be alive on: " + nodeName);
          String coreNodeName = waitForCoreNodeName(collectionName, nodeName, subShardName);
          CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
          cmd.setCoreName(subShardName);
          cmd.setNodeName(nodeName);
          cmd.setCoreNodeName(coreNodeName);
          cmd.setState(ZkStateReader.ACTIVE);
          cmd.setCheckLive(true);
          cmd.setOnlyIfLeader(true);

        ModifiableSolrParams p = new ModifiableSolrParams(cmd.getParams());

        sendShardRequest(nodeName, p, shardHandler, asyncId, requestMap);
      }

        collectShardResponses(results, true,
            "SPLITSHARD timed out waiting for subshard leaders to come up", shardHandler);

        completeAsyncRequest(asyncId, requestMap, results);

        log.info("Successfully created all sub-shards for collection "
            + collectionName + " parent shard: " + slice + " on: " + parentShardLeader);

        log.info("Splitting shard " + parentShardLeader.getName() + " as part of slice "
            + slice + " of collection " + collectionName + " on "
            + parentShardLeader);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
        params.set(CoreAdminParams.CORE, parentShardLeader.getStr("core"));
        for (int i = 0; i < subShardNames.size(); i++) {
          String subShardName = subShardNames.get(i);
          params.add(CoreAdminParams.TARGET_CORE, subShardName);
        }
        params.set(CoreAdminParams.RANGES, rangesStr);

        sendShardRequest(parentShardLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

        collectShardResponses(results, true, "SPLITSHARD failed to invoke SPLIT core admin command",
            shardHandler);
        completeAsyncRequest(asyncId, requestMap, results);

        log.info("Index on shard: " + nodeName + " split into two successfully");

        // apply buffered updates on sub-shards
        for (int i = 0; i < subShardNames.size(); i++) {
          String subShardName = subShardNames.get(i);

          log.info("Applying buffered updates on : " + subShardName);

          params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
          params.set(CoreAdminParams.NAME, subShardName);

          sendShardRequest(nodeName, params, shardHandler, asyncId, requestMap);
        }

        collectShardResponses(results, true,
            "SPLITSHARD failed while asking sub shard leaders to apply buffered updates",
            shardHandler);

        completeAsyncRequest(asyncId, requestMap, results);

        log.info("Successfully applied buffered updates on : " + subShardNames);

        // Replica creation for the new Slices

        // look at the replication factor and see if it matches reality
        // if it does not, find best nodes to create more cores

        // TODO: Have replication factor decided in some other way instead of numShards for the parent

        int repFactor = clusterState.getSlice(collectionName, slice).getReplicas().size();

        // we need to look at every node and see how many cores it serves
        // add our new cores to existing nodes serving the least number of cores
        // but (for now) require that each core goes on a distinct node.

        // TODO: add smarter options that look at the current number of cores per
        // node?
        // for now we just go random
        Set<String> nodes = clusterState.getLiveNodes();
        List<String> nodeList = new ArrayList<>(nodes.size());
        nodeList.addAll(nodes);
      
      Collections.shuffle(nodeList);

        // TODO: Have maxShardsPerNode param for this operation?

        // Remove the node that hosts the parent shard for replica creation.
        nodeList.remove(nodeName);

        // TODO: change this to handle sharding a slice into > 2 sub-shards.

      for (int i = 1; i <= subSlices.size(); i++) {
        Collections.shuffle(nodeList);
        String sliceName = subSlices.get(i - 1);
        for (int j = 2; j <= repFactor; j++) {
          String subShardNodeName = nodeList.get((repFactor * (i - 1) + (j - 2)) % nodeList.size());
          String shardName = collectionName + "_" + sliceName + "_replica" + (j);

            log.info("Creating replica shard " + shardName + " as part of slice "
                + sliceName + " of collection " + collectionName + " on "
                + subShardNodeName);

            HashMap<String, Object> propMap = new HashMap<>();
            propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
            propMap.put(COLLECTION_PROP, collectionName);
            propMap.put(SHARD_ID_PROP, sliceName);
            propMap.put("node", subShardNodeName);
            propMap.put(CoreAdminParams.NAME, shardName);
            // copy over property params:
            for (String key : message.keySet()) {
              if (key.startsWith(COLL_PROP_PREFIX)) {
                propMap.put(key, message.getStr(key));
              }
            }
            // add async param
            if (asyncId != null) {
              propMap.put(ASYNC, asyncId);
            }
            addReplica(clusterState, new ZkNodeProps(propMap), results);

            String coreNodeName = waitForCoreNodeName(collectionName, subShardNodeName, shardName);
            // wait for the replicas to be seen as active on sub shard leader
            log.info("Asking sub shard leader to wait for: " + shardName + " to be alive on: " + subShardNodeName);
            CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
            cmd.setCoreName(subShardNames.get(i - 1));
            cmd.setNodeName(subShardNodeName);
            cmd.setCoreNodeName(coreNodeName);
            cmd.setState(ZkStateReader.RECOVERING);
            cmd.setCheckLive(true);
            cmd.setOnlyIfLeader(true);
            ModifiableSolrParams p = new ModifiableSolrParams(cmd.getParams());

            sendShardRequest(nodeName, p, shardHandler, asyncId, requestMap);

          }
        }

        collectShardResponses(results, true,
            "SPLITSHARD failed to create subshard replicas or timed out waiting for them to come up",
            shardHandler);

        completeAsyncRequest(asyncId, requestMap, results);

        log.info("Successfully created all replica shards for all sub-slices " + subSlices);

        commit(results, slice, parentShardLeader);

      if (repFactor == 1) {
        // switch sub shard states to 'active'
        log.info("Replication factor is 1 so switching shard states");
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
        propMap.put(slice, Slice.INACTIVE);
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.ACTIVE);
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(ZkStateReader.toJSON(m));
      } else  {
        log.info("Requesting shard state be set to 'recovery'");
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.RECOVERY);
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(ZkStateReader.toJSON(m));
      }

        return true;
      } catch (SolrException e) {
        throw e;
      } catch (Exception e) {
        log.error("Error executing split operation for collection: " + collectionName + " parent shard: " + slice, e);
        throw new SolrException(ErrorCode.SERVER_ERROR, null, e);
      
    }
  }

  private void commit(NamedList results, String slice, Replica parentShardLeader) {
    log.info("Calling soft commit to make sub shard updates visible");
    String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl);
      processResponse(results, null, coreUrl, updateResponse, slice, Collections.<String>emptySet());
    } catch (Exception e) {
      processResponse(results, e, coreUrl, updateResponse, slice, Collections.<String>emptySet());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }


  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {
    HttpSolrServer server = null;
    try {
      server = new HttpSolrServer(url);
      server.setConnectionTimeout(30000);
      server.setSoTimeout(120000);
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      return ureq.process(server);
    } finally {
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  private String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore) {
    int retryCount = 320;
    while (retryCount-- > 0) {
      Map<String,Slice> slicesMap = zkStateReader.getClusterState()
          .getSlicesMap(collectionName);
      if (slicesMap != null) {
        
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this
            
            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
            
            if (nodeName.equals(msgNodeName) && core.equals(msgCore)) {
              return replica.getName();
            }
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not find coreNodeName");
  }

  private void waitForNewShard(String collectionName, String sliceName) throws KeeperException, InterruptedException {
    log.info("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    long startTime = System.currentTimeMillis();
    int retryCount = 320;
    while (retryCount-- > 0) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Unable to find collection: " + collectionName + " in clusterstate");
      }
      Slice slice = collection.getSlice(sliceName);
      if (slice != null) {
        log.info("Waited for {} seconds for slice {} of collection {} to be available",
            (System.currentTimeMillis() - startTime) / 1000, sliceName, collectionName);
        return;
      }
      Thread.sleep(1000);
      zkStateReader.updateClusterState(true);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not find new slice " + sliceName + " in collection " + collectionName
            + " even after waiting for " + (System.currentTimeMillis() - startTime) / 1000 + " seconds"
    );
  }

  private void collectShardResponses(NamedList results, boolean abortOnError,
                                     String msgOnError,
                                     ShardHandler shardHandler) {
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp, Collections.<String>emptySet());
        Throwable exception = srsp.getException();
        if (abortOnError && exception != null)  {
          // drain pending requests
          while (srsp != null)  {
            srsp = shardHandler.takeCompletedOrError();
          }
          throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError, exception);
        }
      }
    } while (srsp != null);
  }

  private void deleteShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
      log.info("Delete shard invoked");
      Slice slice = clusterState.getSlice(collection, sliceId);

      if (slice == null) {
        if (clusterState.hasCollection(collection)) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "No shard with name " + sliceId + " exists for collection " + collection);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "No collection with the specified name exists: " + collection);
        }
      }
      // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
      // TODO: Add check for range gaps on Slice deletion
      if (!(slice.getRange() == null || slice.getState().equals(Slice.INACTIVE)
          || slice.getState().equals(Slice.RECOVERY) || slice.getState().equals(Slice.CONSTRUCTION))) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "The slice: " + slice.getName() + " is currently "
                + slice.getState() + ". Only non-active (or custom-hashed) slices can be deleted.");
      }
      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

      try {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
        params.set(CoreAdminParams.DELETE_INDEX, "true");
        sliceCmd(clusterState, params, null, slice, shardHandler);

      processResponses(results, shardHandler, Collections.<String>emptySet());

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          Overseer.REMOVESHARD, ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.SHARD_ID_PROP, sliceId);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));

      // wait for a while until we don't see the shard
      long now = System.nanoTime();
      long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);;
      boolean removed = false;
      while (System.nanoTime() < timeout) {
        Thread.sleep(100);
        removed = zkStateReader.getClusterState().getSlice(collection, sliceId) == null;
        if (removed) {
          Thread.sleep(100); // just a bit of time so it's more likely other readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + collection + " shard: " + sliceId);
      }

        log.info("Successfully deleted collection: " + collection + ", shard: " + sliceId);

      } catch (SolrException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error executing delete operation for collection: " + collection + " shard: " + sliceId, e);
      }
     
  }

  private void migrate(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String sourceCollectionName = message.getStr("collection");
    String splitKey = message.getStr("split.key");
    String targetCollectionName = message.getStr("target.collection");
    int timeout = message.getInt("forward.timeout", 10 * 60) * 1000;

    DocCollection sourceCollection = clusterState.getCollection(sourceCollectionName);
    if (sourceCollection == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown source collection: " + sourceCollectionName);
    }
    DocCollection targetCollection = clusterState.getCollection(targetCollectionName);
    if (targetCollection == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown target collection: " + sourceCollectionName);
    }
    if (!(sourceCollection.getRouter() instanceof CompositeIdRouter))  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Source collection must use a compositeId router");
    }
    if (!(targetCollection.getRouter() instanceof CompositeIdRouter))  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Target collection must use a compositeId router");
    }
    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    CompositeIdRouter targetRouter = (CompositeIdRouter) targetCollection.getRouter();
    Collection<Slice> sourceSlices = sourceRouter.getSearchSlicesSingle(splitKey, null, sourceCollection);
    if (sourceSlices.isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No active slices available in source collection: " + sourceCollection + "for given split.key: " + splitKey);
    }
    Collection<Slice> targetSlices = targetRouter.getSearchSlicesSingle(splitKey, null, targetCollection);
    if (targetSlices.isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No active slices available in target collection: " + targetCollection + "for given split.key: " + splitKey);
    }

    String asyncId = null;
    if(message.containsKey(ASYNC) && message.get(ASYNC) != null)
      asyncId = message.getStr(ASYNC);

    for (Slice sourceSlice : sourceSlices) {
      for (Slice targetSlice : targetSlices) {
        log.info("Migrating source shard: {} to target shard: {} for split.key = " + splitKey, sourceSlice, targetSlice);
        migrateKey(clusterState, sourceCollection, sourceSlice, targetCollection, targetSlice, splitKey,
            timeout, results, asyncId, message);
      }
    }
  }

  private void migrateKey(ClusterState clusterState, DocCollection sourceCollection, Slice sourceSlice,
                          DocCollection targetCollection, Slice targetSlice,
                          String splitKey, int timeout,
                          NamedList results, String asyncId, ZkNodeProps message) throws KeeperException, InterruptedException {
    String tempSourceCollectionName = "split_" + sourceSlice.getName() + "_temp_" + targetSlice.getName();
    if (clusterState.hasCollection(tempSourceCollectionName)) {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      Map<String, Object> props = ZkNodeProps.makeMap(
          Overseer.QUEUE_OPERATION, DELETECOLLECTION,
          "name", tempSourceCollectionName);

      try {
        deleteCollection(new ZkNodeProps(props), results);
        clusterState = zkStateReader.getClusterState();
      } catch (Exception e) {
        log.warn("Unable to clean up existing temporary collection: " + tempSourceCollectionName, e);
      }
    }

    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    DocRouter.Range keyHashRange = sourceRouter.keyHashRange(splitKey);

    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

    log.info("Hash range for split.key: {} is: {}", splitKey, keyHashRange);
    // intersect source range, keyHashRange and target range
    // this is the range that has to be split from source and transferred to target
    DocRouter.Range splitRange = intersect(targetSlice.getRange(), intersect(sourceSlice.getRange(), keyHashRange));
    if (splitRange == null) {
      log.info("No common hashes between source shard: {} and target shard: {}", sourceSlice.getName(), targetSlice.getName());
      return;
    }
    log.info("Common hash range between source shard: {} and target shard: {} = " + splitRange, sourceSlice.getName(), targetSlice.getName());

    Replica targetLeader = zkStateReader.getLeaderRetry(targetCollection.getName(), targetSlice.getName(), 10000);
    // For tracking async calls.
    HashMap<String, String> requestMap = new HashMap<String, String>();

    log.info("Asking target leader node: " + targetLeader.getNodeName() + " core: "
        + targetLeader.getStr("core") + " to buffer updates");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTBUFFERUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));
    String nodeName = targetLeader.getNodeName();

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

    collectShardResponses(results, true, "MIGRATE failed to request node to buffer updates",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, Overseer.ADD_ROUTING_RULE,
        COLLECTION_PROP, sourceCollection.getName(),
        SHARD_ID_PROP, sourceSlice.getName(),
        "routeKey", SolrIndexSplitter.getRouteKey(splitKey) + "!",
        "range", splitRange.toString(),
        "targetCollection", targetCollection.getName(),
        // TODO: look at using nanoTime here?
        "expireAt", String.valueOf(System.currentTimeMillis() + timeout));
    log.info("Adding routing rule: " + m);
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(
        ZkStateReader.toJSON(m));

    // wait for a while until we see the new rule
    log.info("Waiting to see routing rule updated in clusterstate");
    long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    boolean added = false;
    while (System.nanoTime() < waitUntil) {
      Thread.sleep(100);
      Map<String, RoutingRule> rules = zkStateReader.getClusterState().getSlice(sourceCollection.getName(), sourceSlice.getName()).getRoutingRules();
      if (rules != null) {
        RoutingRule rule = rules.get(SolrIndexSplitter.getRouteKey(splitKey) + "!");
        if (rule != null && rule.getRouteRanges().contains(splitRange)) {
          added = true;
          break;
        }
      }
    }
    if (!added) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not add routing rule: " + m);
    }

    log.info("Routing rule added successfully");

    // Create temp core on source shard
    Replica sourceLeader = zkStateReader.getLeaderRetry(sourceCollection.getName(), sourceSlice.getName(), 10000);

    // create a temporary collection with just one node on the shard leader
    String configName = zkStateReader.readConfigName(sourceCollection.getName());
    Map<String, Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION, CREATECOLLECTION,
        "name", tempSourceCollectionName,
        ZkStateReader.REPLICATION_FACTOR, 1,
        NUM_SLICES, 1,
        COLL_CONF, configName,
        CREATE_NODE_SET, sourceLeader.getNodeName());
    if(asyncId != null) {
      String internalAsyncId = asyncId + Math.abs(System.nanoTime());
      props.put(ASYNC, internalAsyncId);
    }

    log.info("Creating temporary collection: " + props);
    createCollection(clusterState, new ZkNodeProps(props), results);
    // refresh cluster state
    clusterState = zkStateReader.getClusterState();
    Slice tempSourceSlice = clusterState.getCollection(tempSourceCollectionName).getSlices().iterator().next();
    Replica tempSourceLeader = zkStateReader.getLeaderRetry(tempSourceCollectionName, tempSourceSlice.getName(), 120000);

    String tempCollectionReplica1 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica1";
    String coreNodeName = waitForCoreNodeName(tempSourceCollectionName,
        sourceLeader.getNodeName(), tempCollectionReplica1);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking source leader to wait for: " + tempCollectionReplica1 + " to be alive on: " + sourceLeader.getNodeName());
    CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempCollectionReplica1);
    cmd.setNodeName(sourceLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(ZkStateReader.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    // we don't want this to happen asynchronously
    sendShardRequest(tempSourceLeader.getNodeName(), new ModifiableSolrParams(cmd.getParams()), shardHandler, null, null);

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection leader or timed out waiting for it to come up",
        shardHandler);

    log.info("Asking source leader to split index");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
    params.set(CoreAdminParams.CORE, sourceLeader.getStr("core"));
    params.add(CoreAdminParams.TARGET_CORE, tempSourceLeader.getStr("core"));
    params.set(CoreAdminParams.RANGES, splitRange.toString());
    params.set("split.key", splitKey);

    String tempNodeName = sourceLeader.getNodeName();

    sendShardRequest(tempNodeName, params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true, "MIGRATE failed to invoke SPLIT core admin command", shardHandler);
    completeAsyncRequest(asyncId, requestMap, results);

    log.info("Creating a replica of temporary collection: {} on the target leader node: {}",
        tempSourceCollectionName, targetLeader.getNodeName());
    String tempCollectionReplica2 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica2";
    props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
    props.put(COLLECTION_PROP, tempSourceCollectionName);
    props.put(SHARD_ID_PROP, tempSourceSlice.getName());
    props.put("node", targetLeader.getNodeName());
    props.put(CoreAdminParams.NAME, tempCollectionReplica2);
    // copy over property params:
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        props.put(key, message.getStr(key));
      }
    }
    // add async param
    if(asyncId != null) {
      props.put(ASYNC, asyncId);
    }
    addReplica(clusterState, new ZkNodeProps(props), results);

    collectShardResponses(results, true,
        "MIGRATE failed to create replica of temporary collection in target leader node.",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    coreNodeName = waitForCoreNodeName(tempSourceCollectionName,
        targetLeader.getNodeName(), tempCollectionReplica2);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking temp source leader to wait for: " + tempCollectionReplica2 + " to be alive on: " + targetLeader.getNodeName());
    cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempSourceLeader.getStr("core"));
    cmd.setNodeName(targetLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(ZkStateReader.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    params = new ModifiableSolrParams(cmd.getParams());

    sendShardRequest(tempSourceLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection replica or timed out waiting for them to come up",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);
    log.info("Successfully created replica of temp source collection on target leader node");

    log.info("Requesting merge of temp source collection replica to target leader");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.MERGEINDEXES.toString());
    params.set(CoreAdminParams.CORE, targetLeader.getStr("core"));
    params.set(CoreAdminParams.SRC_CORE, tempCollectionReplica2);

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true,
        "MIGRATE failed to merge " + tempCollectionReplica2 +
            " to " + targetLeader.getStr("core") + " on node: " + targetLeader.getNodeName(),
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    log.info("Asking target leader to apply buffered updates");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true,
        "MIGRATE failed to request node to apply buffered updates",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    try {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      props = ZkNodeProps.makeMap(
          Overseer.QUEUE_OPERATION, DELETECOLLECTION,
          "name", tempSourceCollectionName);
      deleteCollection(new ZkNodeProps(props), results);
    } catch (Exception e) {
      log.error("Unable to delete temporary collection: " + tempSourceCollectionName
          + ". Please remove it manually", e);
    }
  }

  private void completeAsyncRequest(String asyncId, HashMap<String, String> requestMap, NamedList results) {
    if(asyncId != null) {
      waitForAsyncCallsToComplete(requestMap, results);
      requestMap.clear();
    }
  }

  private DocRouter.Range intersect(DocRouter.Range a, DocRouter.Range b) {
    if (a == null || b == null || !a.overlaps(b)) {
      return null;
    } else if (a.isSubsetOf(b))
      return a;
    else if (b.isSubsetOf(a))
      return b;
    else if (b.includes(a.max)) {
      return new DocRouter.Range(b.min, a.max);
    } else  {
      return new DocRouter.Range(a.min, b.max);
    }
  }

  private void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler, String asyncId, Map<String, String> requestMap) {
    if (asyncId != null) {
      String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
      params.set(ASYNC, coreAdminAsyncId);
      requestMap.put(nodeName, coreAdminAsyncId);
    }

    ShardRequest sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;

    shardHandler.submit(sreq, replica, sreq.params);
  }

  private void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  private void addPropertyParams(ZkNodeProps message, Map<String,Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }

 private void createCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String collectionName = message.getStr("name");
    if (clusterState.hasCollection(collectionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }
    
    try {
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores
      
      int repFactor = message.getInt(ZkStateReader.REPLICATION_FACTOR, 1);

      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
      String async = null;
      async = message.getStr("async");

      Integer numSlices = message.getInt(NUM_SLICES, null);
      String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);
      List<String> shardNames = new ArrayList<>();
      if(ImplicitDocRouter.NAME.equals(router)){
        Overseer.getShardNames(shardNames, message.getStr("shards",null));
        numSlices = shardNames.size();
      } else {
        Overseer.getShardNames(numSlices,shardNames);
      }

      if (numSlices == null ) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " is a required param (when using CompositeId router).");
      }

      int maxShardsPerNode = message.getInt(ZkStateReader.MAX_SHARDS_PER_NODE, 1);
      String createNodeSetStr = message.getStr(CREATE_NODE_SET);
      List<String> createNodeList = (createNodeSetStr == null)?null:StrUtils.splitSmart((CREATE_NODE_SET_EMPTY.equals(createNodeSetStr)?"":createNodeSetStr), ",", true);
      if (repFactor <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ZkStateReader.REPLICATION_FACTOR + " must be greater than 0");
      }
      
      if (numSlices <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " must be > 0");
      }
      
      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.
      
      // TODO: add smarter options that look at the current number of cores per
      // node?
      // for now we just go random
      Set<String> nodes = clusterState.getLiveNodes();
      List<String> nodeList = new ArrayList<>(nodes.size());
      nodeList.addAll(nodes);
      if (createNodeList != null) nodeList.retainAll(createNodeList);
      Collections.shuffle(nodeList);

      boolean corelessCollection = CREATE_NODE_SET_EMPTY.equals(createNodeSetStr);

      if (corelessCollection) {
        log.warn("It is unusual to create a collection ("+collectionName+") without cores.");

      } else {
        if (nodeList.size() <= 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName
              + ". No live Solr-instances" + ((createNodeList != null)?" among Solr-instances specified in " + CREATE_NODE_SET + ":" + createNodeSetStr:""));
        }

        if (repFactor > nodeList.size()) {
          log.warn("Specified "
              + ZkStateReader.REPLICATION_FACTOR
              + " of "
              + repFactor
              + " on collection "
              + collectionName
              + " is higher than or equal to the number of Solr instances currently live or part of your " + CREATE_NODE_SET + "("
              + nodeList.size()
              + "). Its unusual to run two replica of the same slice on the same Solr-instance.");
        }

        int maxShardsAllowedToCreate = maxShardsPerNode * nodeList.size();
        int requestedShardsToCreate = numSlices * repFactor;
        if (maxShardsAllowedToCreate < requestedShardsToCreate) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName + ". Value of "
              + ZkStateReader.MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
              + ", and the number of live nodes is " + nodeList.size()
              + ". This allows a maximum of " + maxShardsAllowedToCreate
              + " to be created. Value of " + NUM_SLICES + " is " + numSlices
              + " and value of " + ZkStateReader.REPLICATION_FACTOR + " is " + repFactor
              + ". This requires " + requestedShardsToCreate
              + " shards to be created (higher than the allowed number)");
        }
      }

      boolean isLegacyCloud =  Overseer.isLegacy(zkStateReader.getClusterProps());

      String configName = createConfNode(collectionName, message, isLegacyCloud);

      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));

      // wait for a while until we don't see the collection
      long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean created = false;
      while (System.nanoTime() < waitUntil) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().getCollections().contains(message.getStr("name"));
        if(created) break;
      }
      if (!created)
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully createcollection: " + message.getStr("name"));

      if (corelessCollection) {
        log.info("Finished create command for collection: {}", collectionName);
        return;
      }

      // For tracking async calls.
      HashMap<String, String> requestMap = new HashMap<String, String>();

      log.info("Creating SolrCores for new collection {}, shardNames {} , replicationFactor : {}",
          collectionName, shardNames, repFactor);
      Map<String ,ShardRequest> coresToCreate = new LinkedHashMap<>();
      for (int i = 1; i <= shardNames.size(); i++) {
        String sliceName = shardNames.get(i-1);
        for (int j = 1; j <= repFactor; j++) {
          String nodeName = nodeList.get((repFactor * (i - 1) + (j - 1)) % nodeList.size());
          String coreName = collectionName + "_" + sliceName + "_replica" + j;
          log.info("Creating shard " + coreName + " as part of slice "
              + sliceName + " of collection " + collectionName + " on "
              + nodeName);


          String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
          //in the new mode, create the replica in clusterstate prior to creating the core.
          // Otherwise the core creation fails
          if(!isLegacyCloud){
            ZkNodeProps props = new ZkNodeProps(
                Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.ADDREPLICA.toString(),
                ZkStateReader.COLLECTION_PROP, collectionName,
                ZkStateReader.SHARD_ID_PROP, sliceName,
                ZkStateReader.CORE_NAME_PROP, coreName,
                ZkStateReader.STATE_PROP, ZkStateReader.DOWN,
                ZkStateReader.BASE_URL_PROP,baseUrl);
                Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(props));
          }

          // Need to create new params for each request
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

          params.set(CoreAdminParams.NAME, coreName);
          params.set(COLL_CONF, configName);
          params.set(CoreAdminParams.COLLECTION, collectionName);
          params.set(CoreAdminParams.SHARD, sliceName);
          params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

          if (async != null)  {
            String coreAdminAsyncId = async + Math.abs(System.nanoTime());
            params.add(ASYNC, coreAdminAsyncId);
            requestMap.put(nodeName, coreAdminAsyncId);
          }
          addPropertyParams(message, params);

          ShardRequest sreq = new ShardRequest();
          params.set("qt", adminPath);
          sreq.purpose = 1;
          sreq.shards = new String[] {baseUrl};
          sreq.actualShards = sreq.shards;
          sreq.params = params;

          if(isLegacyCloud) {
            shardHandler.submit(sreq, sreq.shards[0], sreq.params);
          } else {
            coresToCreate.put(coreName, sreq);
          }
        }
      }

      if(!isLegacyCloud) {
        // wait for all replica entries to be created
        Map<String, Replica> replicas = waitToSeeReplicasInState(collectionName, coresToCreate.keySet());
        for (Map.Entry<String, ShardRequest> e : coresToCreate.entrySet()) {
          ShardRequest sreq = e.getValue();
          sreq.params.set(CoreAdminParams.CORE_NODE_NAME, replicas.get(e.getKey()).getName());
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        }
      }

      processResponses(results, shardHandler, Collections.<String>emptySet());

      completeAsyncRequest(async, requestMap, results);

      log.info("Finished create command on all shards for collection: "
          + collectionName);

    } catch (SolrException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR, null, ex);
    }
  }

  private Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    Map<String, Replica> result = new HashMap<>();
    long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    while (true) {
      DocCollection coll = zkStateReader.getClusterState().getCollection(
          collectionName);
      for (String coreName : coreNames) {
        if (result.containsKey(coreName)) continue;
        for (Slice slice : coll.getSlices()) {
          for (Replica replica : slice.getReplicas()) {
            if (coreName.equals(replica.getStr(ZkStateReader.CORE_NAME_PROP))) {
              result.put(coreName, replica);
              break;
            }
          }
        }
      }
      
      if (result.size() == coreNames.size()) {
        return result;
      }
      if (System.nanoTime() > endTime) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out waiting to see all replicas in cluster state.");
      }
      
      Thread.sleep(100);
    }
  }

  private void addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collection = message.getStr(COLLECTION_PROP);
    String node = message.getStr("node");
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
      String asyncId = message.getStr("async");

      DocCollection coll = clusterState.getCollection(collection);
      if (coll == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
      }
      if (coll.getSlice(shard) == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Collection: " + collection + " shard: " + shard + " does not exist");
      }
      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

      if (node == null) {
        node = getNodesForNewShard(clusterState, collection, coll.getSlices().size(), coll.getInt(ZkStateReader.MAX_SHARDS_PER_NODE, 1), coll.getInt(ZkStateReader.REPLICATION_FACTOR, 1), null).get(0).nodeName;
        log.info("Node not provided, Identified {} for creating new replica", node);
      }


      if (!clusterState.liveNodesContain(node)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Node: " + node + " is not live");
      }
      if (coreName == null) {
        // assign a name to this core
        Slice slice = coll.getSlice(shard);
        int replicaNum = slice.getReplicas().size();
        for (; ; ) {
          String replicaName = collection + "_" + shard + "_replica" + replicaNum;
          boolean exists = false;
          for (Replica replica : slice.getReplicas()) {
            if (replicaName.equals(replica.getStr("core"))) {
              exists = true;
              break;
            }
          }
          if (exists) replicaNum++;
          else break;
        }
        coreName = collection + "_" + shard + "_replica" + replicaNum;
      }
      ModifiableSolrParams params = new ModifiableSolrParams();

    if(!Overseer.isLegacy(zkStateReader.getClusterProps())){
      ZkNodeProps props = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.SHARD_ID_PROP, shard,
          ZkStateReader.CORE_NAME_PROP, coreName,
          ZkStateReader.STATE_PROP, ZkStateReader.DOWN,
          ZkStateReader.BASE_URL_PROP,zkStateReader.getBaseUrlForNodeName(node));
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(props));
      params.set(CoreAdminParams.CORE_NODE_NAME, waitToSeeReplicasInState(collection, Collections.singletonList(coreName)).get(coreName).getName());
    }


      String configName = zkStateReader.readConfigName(collection);
      String routeKey = message.getStr(ShardParams._ROUTE_);
      String dataDir = message.getStr(CoreAdminParams.DATA_DIR);
      String instanceDir = message.getStr(CoreAdminParams.INSTANCE_DIR);

      params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
      params.set(CoreAdminParams.NAME, coreName);
      params.set(COLL_CONF, configName);
      params.set(CoreAdminParams.COLLECTION, collection);
      if (shard != null) {
        params.set(CoreAdminParams.SHARD, shard);
      } else if (routeKey != null) {
        Collection<Slice> slices = coll.getRouter().getSearchSlicesSingle(routeKey, null, coll);
        if (slices.isEmpty()) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No active shard serving _route_=" + routeKey + " found");
        } else {
          params.set(CoreAdminParams.SHARD, slices.iterator().next().getName());
        }
      } else  {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Specify either 'shard' or _route_ param");
      }
      if (dataDir != null) {
        params.set(CoreAdminParams.DATA_DIR, dataDir);
      }
      if (instanceDir != null) {
        params.set(CoreAdminParams.INSTANCE_DIR, instanceDir);
      }
      addPropertyParams(message, params);

      // For tracking async calls.
      HashMap<String, String> requestMap = new HashMap<>();
      sendShardRequest(node, params, shardHandler, asyncId, requestMap);

      collectShardResponses(results, true,
          "ADDREPLICA failed to create replica", shardHandler);

      completeAsyncRequest(asyncId, requestMap, results);
  }

  private void processCreateSnapshotAction(ZkNodeProps message, NamedList result) throws IOException, KeeperException, InterruptedException {
    String collectionName =  message.getStr(COLLECTION_PROP);
    String commitName =  message.getStr(CoreAdminParams.COMMIT_NAME);
    String asyncId = message.getStr(ASYNC);
    SolrZkClient zkClient = this.overseer.getZkController().getZkClient();
    long creationTime = System.currentTimeMillis();

    if(SolrSnapshotManager.snapshotExists(zkClient, collectionName, commitName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName
          + " already exists for collection " + collectionName);
    }

    log.info("Creating a snapshot for collection={} with commitName={}", collectionName, commitName);

    // Create a node in ZK to store the collection level snapshot meta-data.
    SolrSnapshotManager.createCollectionLevelSnapshot(zkClient, collectionName, new CollectionSnapshotMetaData(commitName));
    log.info("Created a ZK path to store snapshot information for collection={} with commitName={}", collectionName, commitName);

    Map<String, String> requestMap = new HashMap<>();
    NamedList shardRequestResults = new NamedList();
    Map<String, Slice> shardByCoreName = new HashMap<>();
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

    for (Slice slice : zkStateReader.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        if (replica.getState().equals(ZkStateReader.DOWN)) {
          log.info("Replica {} is down. Hence not sending the createsnapshot request", replica.getCoreName());
          continue; // Since replica is down - no point sending a request.
        }

        String coreName = replica.getStr(CORE_NAME_PROP);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATESNAPSHOT.toString());
        params.set(NAME, slice.getName());
        params.set(CORE_NAME_PROP, coreName);
        params.set(CoreAdminParams.COMMIT_NAME, commitName);

        sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap);
        log.debug("Sent createsnapshot request to core={} with commitName={}", coreName, commitName);

        shardByCoreName.put(coreName, slice);
      }
    }

    // At this point we want to make sure that at-least one replica for every shard
    // is able to create the snapshot. If that is not the case, then we fail the request.
    // This is to take care of the situation where e.g. entire shard is unavailable.
    Set<String> failedShards = new HashSet<>();

    processResponses(shardRequestResults, shardHandler, false, null, asyncId, requestMap);
    NamedList success = (NamedList) shardRequestResults.get("success");
    List<CoreSnapshotMetaData> replicas = new ArrayList<>();
    if (success != null) {
      for ( int i = 0 ; i < success.size() ; i++) {
        NamedList resp = (NamedList)success.getVal(i);

        // Check if this core is the leader for the shard. The idea here is that during the backup
        // operation we preferably use the snapshot of the "leader" replica since it is most likely
        // to have latest state.
        String coreName = (String)resp.get(CoreAdminParams.CORE);
        Slice slice = shardByCoreName.remove(coreName);
        boolean leader = (slice.getLeader() != null && slice.getLeader().getCoreName().equals(coreName));
        resp.add(SolrSnapshotManager.SHARD_ID, slice.getName());
        resp.add(SolrSnapshotManager.LEADER, leader);

        CoreSnapshotMetaData c = new CoreSnapshotMetaData(resp);
        replicas.add(c);
        log.info("Snapshot with commitName {} is created successfully for core {}", commitName, c.getCoreName());
      }
    }

    if (!shardByCoreName.isEmpty()) { // One or more failures.
      log.warn("Unable to create a snapshot with name {} for following cores {}", commitName, shardByCoreName.keySet());

      // Count number of failures per shard.
      Map<String, Integer> failuresByShardId = new HashMap<>();
      for (Map.Entry<String,Slice> entry : shardByCoreName.entrySet()) {
        int f = 0;
        if (failuresByShardId.get(entry.getValue().getName()) != null) {
          f = failuresByShardId.get(entry.getValue().getName());
        }
        failuresByShardId.put(entry.getValue().getName(), f + 1);
      }

      // Now that we know number of failures per shard, we can figure out
      // if at-least one replica per shard was able to create a snapshot or not.
      DocCollection collectionStatus = zkStateReader.getClusterState().getCollection(collectionName);
      for (Map.Entry<String,Integer> entry : failuresByShardId.entrySet()) {
        int replicaCount = collectionStatus.getSlice(entry.getKey()).getReplicas().size();
        if (replicaCount <= entry.getValue()) {
          failedShards.add(entry.getKey());
        }
      }
    }

    if (failedShards.isEmpty()) { // No failures.
      CollectionSnapshotMetaData meta = new CollectionSnapshotMetaData(commitName, SnapshotStatus.Successful, creationTime, replicas);
      SolrSnapshotManager.updateCollectionLevelSnapshot(zkClient, collectionName, meta);
      log.info("Saved following snapshot information for collection={} with commitName={} in Zookeeper : {}", collectionName,
          commitName, meta.toNamedList());
    } else {
      log.warn("Failed to create a snapshot for collection {} with commitName = {}. Snapshot could not be captured for following shards {}",
          collectionName, commitName, failedShards);
      // Update the ZK meta-data to include only cores with the snapshot. This will enable users to figure out
      // which cores have the named snapshot.
      CollectionSnapshotMetaData meta = new CollectionSnapshotMetaData(commitName, SnapshotStatus.Failed, creationTime, replicas);
      SolrSnapshotManager.updateCollectionLevelSnapshot(zkClient, collectionName, meta);
      log.info("Saved following snapshot information for collection={} with commitName={} in Zookeeper : {}", collectionName,
          commitName, meta.toNamedList());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to create snapshot on shards " + failedShards);
    }
  }

  private void processDeleteSnapshotAction(ZkNodeProps message, NamedList results) throws IOException, KeeperException, InterruptedException {
    String collectionName =  message.getStr(COLLECTION_PROP);
    String commitName =  message.getStr(CoreAdminParams.COMMIT_NAME);
    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = new HashMap<>();
    NamedList shardRequestResults = new NamedList();
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    SolrZkClient zkClient = this.overseer.getZkController().getZkClient();

    Optional<CollectionSnapshotMetaData> meta = SolrSnapshotManager.getCollectionLevelSnapshot(zkClient, collectionName, commitName);
    if (!meta.isPresent()) { // Snapshot not found. Nothing to do.
      return;
    }

    log.info("Deleting a snapshot for collection={} with commitName={}", collectionName, commitName);

    Set<String> existingCores = new HashSet<>();
    for (Slice s : zkStateReader.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica r : s.getReplicas()) {
        existingCores.add(r.getCoreName());
      }
    }

    Set<String> coresWithSnapshot = new HashSet<>();
    for (CoreSnapshotMetaData m : meta.get().getReplicaSnapshots()) {
      if (existingCores.contains(m.getCoreName())) {
        coresWithSnapshot.add(m.getCoreName());
      }
    }

    log.info("Existing cores with snapshot for collection={} are {}", collectionName, existingCores);
    for (Slice slice : zkStateReader.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        if (replica.getState().equals(ZkStateReader.DOWN)) {
          continue; // Since replica is down - no point sending a request.
        }

        // Note - when a snapshot is found in_progress state - it is the result of overseer
        // failure while handling the snapshot creation. Since we don't know the exact set of
        // replicas to contact at this point, we try on all replicas.
        if (meta.get().getStatus() == SnapshotStatus.InProgress || coresWithSnapshot.contains(replica.getCoreName())) {
          String coreName = replica.getStr(CORE_NAME_PROP);

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.DELETESNAPSHOT.toString());
          params.set(NAME, slice.getName());
          params.set(CORE_NAME_PROP, coreName);
          params.set(CoreAdminParams.COMMIT_NAME, commitName);

          log.info("Sending deletesnapshot request to core={} with commitName={}", coreName, commitName);
          sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap);
        }
      }
    }

    processResponses(shardRequestResults, shardHandler, false, null, asyncId, requestMap);
    NamedList success = (NamedList) shardRequestResults.get("success");
    List<CoreSnapshotMetaData> replicas = new ArrayList<>();
    if (success != null) {
      for ( int i = 0 ; i < success.size() ; i++) {
        NamedList resp = (NamedList)success.getVal(i);
        // Unfortunately async processing logic doesn't provide the "core" name automatically.
        String coreName = (String)resp.get("core");
        coresWithSnapshot.remove(coreName);
      }
    }

    if (!coresWithSnapshot.isEmpty()) { // One or more failures.
      log.warn("Failed to delete a snapshot for collection {} with commitName = {}. Snapshot could not be deleted for following cores {}",
          collectionName, commitName, coresWithSnapshot);

      List<CoreSnapshotMetaData> replicasWithSnapshot = new ArrayList<>();
      for (CoreSnapshotMetaData m : meta.get().getReplicaSnapshots()) {
        if (coresWithSnapshot.contains(m.getCoreName())) {
          replicasWithSnapshot.add(m);
        }
      }

      // Update the ZK meta-data to include only cores with the snapshot. This will enable users to figure out
      // which cores still contain the named snapshot.
      CollectionSnapshotMetaData newResult = new CollectionSnapshotMetaData(meta.get().getName(), SnapshotStatus.Failed,
          meta.get().getCreationTime(), replicasWithSnapshot);
      SolrSnapshotManager.updateCollectionLevelSnapshot(zkClient, collectionName, newResult);
      log.info("Saved snapshot information for collection={} with commitName={} in Zookeeper as follows", collectionName, commitName,
          Utils.toJSON(newResult));
      throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to delete snapshot on cores " + coresWithSnapshot);

    } else {
      // Delete the ZK path so that we eliminate the references of this snapshot from collection level meta-data.
      SolrSnapshotManager.deleteCollectionLevelSnapshot(zkClient, collectionName, commitName);
      log.info("Deleted Zookeeper snapshot metdata for collection={} with commitName={}", collectionName, commitName);
      log.info("Successfully deleted snapshot for collection={} with commitName={}", collectionName, commitName);
    }
  }

  private IndexBackupStrategy newIndexBackupStrategy(ZkNodeProps request) throws InterruptedException, KeeperException {
    String strategy = request.getStr(CollectionAdminParams.INDEX_BACKUP_STRATEGY, CollectionAdminParams.COPY_FILES_STRATEGY);
    switch (strategy) {
      case CollectionAdminParams.COPY_FILES_STRATEGY: {
        String collectionName =  request.getStr(COLLECTION_PROP);
        String repo = request.getStr(CoreAdminParams.BACKUP_REPOSITORY);
        String asyncId = request.getStr(ASYNC);
        ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
        ShardRequestProcessor processor = new ShardRequestProcessor(shardHandler, adminPath, zkStateReader, asyncId);

        String commitName = request.getStr(CoreAdminParams.COMMIT_NAME);
        Optional<CollectionSnapshotMetaData> snapshotMeta = Optional.absent();
        if (commitName != null) {
          SolrZkClient zkClient = this.overseer.getZkController().getZkClient();
          snapshotMeta = SolrSnapshotManager.getCollectionLevelSnapshot(zkClient, collectionName, commitName);
          if (!snapshotMeta.isPresent()) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName
                + " does not exist for collection " + collectionName);
          }
          if (snapshotMeta.get().getStatus() != SnapshotStatus.Successful) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName + " for collection " + collectionName
                + " has not completed successfully. The status is " + snapshotMeta.get().getStatus());
          }
        }

        return new CopyFilesStrategy(processor, repo, snapshotMeta);
      }
      case CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY: {
        return new NoIndexBackupStrategy();
      }
      default: {
        throw new IllegalStateException("Unknown index backup strategy "+strategy);
      }
    }
  }


  private void processBackupAction(ZkNodeProps message, NamedList results) throws IOException, KeeperException, InterruptedException {
    String collectionName =  message.getStr(COLLECTION_PROP);
    String backupName =  message.getStr(NAME);
    String location = message.getStr(CoreAdminParams.BACKUP_LOCATION);
    String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);

    CoreContainer cc = this.overseer.getZkController().getCoreContainer();
    BackupRepository repository = cc.newBackupRepository(Optional.fromNullable(repo));
    BackupManager backupMgr = new BackupManager(repository, zkStateReader, collectionName);
    IndexBackupStrategy strategy = newIndexBackupStrategy(message);

    // Backup location
    URI backupPath = repository.createURI(location, backupName);

    //Validating if the directory already exists.
    if (repository.exists(backupPath)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "The backup directory already exists: " + backupPath);
    }

    // Create a directory to store backup details.
    repository.createDirectory(backupPath);

    strategy.createBackup(backupPath, collectionName, backupName);

    log.info("Starting to backup ZK data for backupName={}", backupName);

    //Download the configs
    String configName = zkStateReader.readConfigName(collectionName);
    backupMgr.downloadConfigDir(location, backupName, configName);

    //Save the collection's state. Can be part of the monolithic clusterstate.json or a individual state.json
    //Since we don't want to distinguish we extract the state and back it up as a separate json
    DocCollection collectionState = zkStateReader.getClusterState().getCollection(collectionName);
    backupMgr.writeCollectionState(location, backupName, collectionName, collectionState);

    Properties properties = new Properties();

    properties.put(BackupManager.BACKUP_NAME_PROP, backupName);
    properties.put(BackupManager.COLLECTION_NAME_PROP, collectionName);
    properties.put(COLL_CONF, configName);
    properties.put(BackupManager.START_TIME_PROP, (new Date()).toString());
    properties.put(BackupManager.INDEX_VERSION_PROP, Version.LATEST.toString());
    //TODO: Add MD5 of the configset. If during restore the same name configset exists then we can compare checksums to see if they are the same.
    //if they are not the same then we can throw an error or have an 'overwriteConfig' flag
    //TODO save numDocs for the shardLeader. We can use it to sanity check the restore.

    backupMgr.writeBackupProperties(location, backupName, properties);

    log.info("Completed backing up ZK data for backupName={}", backupName);
  }

  private <T> T getOrDefault(T val, T defaultVal) {
    return (val != null) ? val : defaultVal;
  }

  private void processRestoreAction(ZkNodeProps message, NamedList results) throws IOException, KeeperException, InterruptedException {
    // TODO maybe we can inherit createCollection's options/code
    String restoreCollectionName =  message.getStr(COLLECTION_PROP);
    String backupName =  message.getStr(NAME); // of backup
    String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
    String location = message.getStr(CoreAdminParams.BACKUP_LOCATION);
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = new HashMap<>();

    CoreContainer cc = this.overseer.getZkController().getCoreContainer();
    BackupRepository repository = cc.newBackupRepository(Optional.fromNullable(repo));

    URI backupPath = repository.createURI(location, backupName);
    BackupManager backupMgr = new BackupManager(repository, zkStateReader, restoreCollectionName);

    Properties properties = backupMgr.readBackupProperties(location, backupName);
    String backupCollection = properties.getProperty(BackupManager.COLLECTION_NAME_PROP);
    DocCollection backupCollectionState = backupMgr.readCollectionState(location, backupName, backupCollection);

    // Get the Solr nodes to restore a collection.
    final List<String> nodeList = getLiveOrLiveAndCreateNodeSetList(
        zkStateReader.getClusterState().getLiveNodes(), message);

    int numShards = backupCollectionState.getActiveSlices().size();
    int repFactor = message.getInt(REPLICATION_FACTOR,
        getOrDefault(backupCollectionState.getReplicationFactor(), 1));
    int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE,
        getOrDefault(backupCollectionState.getMaxShardsPerNodeOrNull(), 1));
    int availableNodeCount = nodeList.size();
    if ((numShards * repFactor) > (availableNodeCount * maxShardsPerNode)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "Solr cloud with available number of nodes:%d is insufficient for"
              + " restoring a collection with %d shards, replication factor %d and maxShardsPerNode %d."
              + " Consider increasing maxShardsPerNode value OR number of available nodes.",
              availableNodeCount, numShards, repFactor, maxShardsPerNode));
    }

    //Upload the configs
    String configName = (String) properties.get(COLL_CONF);
    String restoreConfigName = message.getStr(COLL_CONF, configName);
    if (zkStateReader.getConfigManager().configExists(restoreConfigName)) {
      log.info("Using existing config {}", restoreConfigName);
      //TODO add overwrite option?
    } else {
      log.info("Uploading config {}", restoreConfigName);
      backupMgr.uploadConfigDir(location, backupName, configName, restoreConfigName);
    }

    log.info("Starting restore into collection={} with backup_name={} at location={}", restoreCollectionName, backupName,
        location);

    //Create core-less collection
    {
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATECOLLECTION);
      propMap.put("fromApi", "true"); // mostly true.  Prevents autoCreated=true in the collection state.

      // inherit settings from input API, defaulting to the backup's setting.  Ex: replicationFactor
      for (String collProp : COLL_PROPS.keySet()) {
        Object val = message.getProperties().get(collProp);
        if (val == null) {
          val = backupCollectionState.get(collProp);
        }
        if (val != null) {
          propMap.put(collProp, val);
        }
      }

      propMap.put(NAME, restoreCollectionName);
      propMap.put(CREATE_NODE_SET, CREATE_NODE_SET_EMPTY); //no cores
      propMap.put(COLL_CONF, restoreConfigName);

      String prefix = ROUTER.concat(".");
      // new router format
      if (backupCollectionState.getProperties().get(DocCollection.DOC_ROUTER) instanceof Map) {
        // router.*
        @SuppressWarnings("unchecked")
        Map<String, Object> routerProps = (Map<String, Object>) backupCollectionState
            .getProperties().get(DocCollection.DOC_ROUTER);
        for (Map.Entry<String, Object> pair : routerProps.entrySet()) {
          propMap.put(prefix + pair.getKey(), pair.getValue());
        }
      } else if (backupCollectionState.getProperties().get(DocCollection.DOC_ROUTER_OLD) != null) {
        // read config in old router format and convert it to new format.
        propMap.put(prefix+"name",
            backupCollectionState.getProperties().get(DocCollection.DOC_ROUTER_OLD));
      }

      Set<String> sliceNames = backupCollectionState.getActiveSlicesMap().keySet();
      if (backupCollectionState.getRouter() instanceof ImplicitDocRouter) {
        propMap.put(SHARDS_PROP, StrUtils.join(sliceNames, ','));
      } else {
        propMap.put(NUM_SLICES, sliceNames.size());
        // ClusterStateMutator.createCollection detects that "slices" is in fact a slice structure instead of a
        //   list of names, and if so uses this instead of building it.  We clear the replica list.
        Collection<Slice> backupSlices = backupCollectionState.getActiveSlices();
        Map<String,Slice> newSlices = new LinkedHashMap<>(backupSlices.size());
        for (Slice backupSlice : backupSlices) {
          newSlices.put(backupSlice.getName(),
              new Slice(backupSlice.getName(), Collections.<String, Replica>emptyMap(), backupSlice.getProperties()));
        }
        propMap.put(SHARDS_PROP, newSlices);
      }

      createCollection(zkStateReader.getClusterState(), new ZkNodeProps(propMap), new NamedList());
      // note: when createCollection() returns, the collection exists (no race)
    }

    DocCollection restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());

    //Mark all shards in CONSTRUCTION STATE while we restore the data
    {
      //TODO might instead createCollection accept an initial state?  Is there a race?
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, Overseer.UPDATESHARDSTATE.toLowerCase(Locale.ENGLISH));
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.CONSTRUCTION.toString());
      }
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
      inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    // TODO how do we leverage the RULE / SNITCH logic in createCollection?

    ClusterState clusterState = zkStateReader.getClusterState();
    Map<String, List<String>> hostsByShardName = shardToHostAssignment(nodeList, restoreCollection);

    //Create one replica per shard and copy backed up data to it
    for (Slice slice: restoreCollection.getSlices()) {
      List<String> nodes = hostsByShardName.get(slice.getName());
      String nodeName = nodes.get(0);

      log.info("Adding replica for shard={} collection={} on node {}",
          slice.getName(), restoreCollection.getName(), nodeName);
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD);
      propMap.put(COLLECTION_PROP, restoreCollectionName);
      propMap.put(SHARD_ID_PROP, slice.getName());
      propMap.put("node", nodeName);
      // add async param
      if (asyncId != null) {
        propMap.put(ASYNC, asyncId);
      }
      addPropertyParams(message, propMap);

      addReplica(clusterState, new ZkNodeProps(propMap), new NamedList());
    }

    //refresh the location copy of collection state
    restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    //Copy data from backed up index to each replica
    for (Slice slice: restoreCollection.getSlices()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.RESTORECORE.toString());
      params.set(NAME, "snapshot." + slice.getName());
      params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.getPath());
      params.set(CoreAdminParams.BACKUP_REPOSITORY, repo);
      sliceCmd(clusterState, params, (String) null, slice, shardHandler, asyncId, requestMap);
    }
    processResponses(new NamedList(), shardHandler, true, "Could not restore core", asyncId, requestMap);

    //Mark all shards in ACTIVE STATE
    {
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, Overseer.UPDATESHARDSTATE.toLowerCase(Locale.ENGLISH));
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.ACTIVE.toString());
      }
      inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    //refresh the location copy of collection state
    restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    //Add the remaining replicas for each shard
    Integer numReplicas = restoreCollection.getReplicationFactor();
    if (numReplicas != null && numReplicas > 1) {
      log.info("Adding replicas to restored collection={}", restoreCollection);

      for (Slice slice: restoreCollection.getSlices()) {
        List<String> nodes = hostsByShardName.get(slice.getName());

        for(int i=1; i<numReplicas; i++) {
          String nodeName = nodes.get(i);

          log.info("Adding replica for shard={} collection={} on node {}",
              slice.getName(), restoreCollection.getName(), nodeName);

          HashMap<String, Object> propMap = new HashMap<>();
          propMap.put(COLLECTION_PROP, restoreCollectionName);
          propMap.put(SHARD_ID_PROP, slice.getName());
          propMap.put("node", nodeName);
          // add async param
          if (asyncId != null) {
            propMap.put(ASYNC, asyncId);
          }
          addPropertyParams(message, propMap);

          addReplica(zkStateReader.getClusterState(), new ZkNodeProps(propMap), results);
        }
      }
    }

    log.info("Completed restoring collection={} backupName={}", restoreCollection, backupName);
  }

  /**
   * This method prepares the assignment of hosts in the solr cluster for the
   * replicas in the collection to be restored. It attempts to satisfy two constraints
   * (a) No host should be assigned more than one replica for a given shard
   * (b) First replica for every shard should be on a distinct host
   *
   * @param nodeList List of live (and chosen) hosts in Solr cluster
   * @param restoreCollection The metadata for the collection to be restored.
   * @return A map which has shard name as the key and list of hosts as the value.
   *         The caller of this function should use the list of hosts to assign replicas
   *         for the given shard (specified by the key).
   */
  private static Map<String, List<String>> shardToHostAssignment (List<String> nodeList,
      DocCollection restoreCollection) {
    Integer numReplicas = restoreCollection.getReplicationFactor();
    Map<String, List<String>> result = new HashMap<>();
    List<String> nodesWithFirstReplica = new ArrayList<>();

    if (nodeList.size() < numReplicas) {
      log.warn("Number of replicas per shard for collection {} are more than number of"
          + " live (or chosen) nodes in the cluster. replication factor : {} number of nodes {}. "
          + " This will place more than one replica for a given shard on the same node.",
          restoreCollection.getName(), numReplicas, nodeList.size());
    }

    List<Slice> slices = new ArrayList<>(restoreCollection.getSlices());
    Set<String> hostsWithFirstReplica = new HashSet<>();
    for (int i = 0; i < slices.size(); i++) {
      String name = slices.get(i).getName();
      List<String> nodes = new ArrayList<>();
      for (int j = 0; j < numReplicas; j++) {
        int index = (i * numReplicas + j) % nodeList.size();
        String node = nodeList.get(index);
        nodes.add(node);
      }
      if (!nodes.isEmpty()) {
        if (hostsWithFirstReplica.contains(nodes.get(0))) {
          boolean shuffle = false;
          for (int j = 1; i < nodes.size() && !shuffle; i++) {
            String node = nodes.get(j);
            if (!hostsWithFirstReplica.contains(node)) {
              // Found a node which is not currently hosting first replica for any shard.
              // shuffle the position in the list so that this node can take over hosting
              // first replica for this shard.
              log.info("Shuffling {} and {} to ensure any node is not assigned first replica"
                  + " for more than one shard during restore operation of collection {}"
                  , nodes.get(0), nodes.get(j), restoreCollection.getName());
              nodes.set(j, nodes.get(0));
              nodes.set(0, node);
              shuffle = true;
            }
          }
          if (!shuffle) {
            log.warn("The node {} is configured to host first replica for more than"
                + " one shard during restore operation for collection {}", nodes.get(0), restoreCollection.getName());
          }
        }
        hostsWithFirstReplica.add(nodes.get(0));
      }
      result.put(name, nodes);
    }

    return result;
  }

  static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message) {
    // TODO: add smarter options that look at the current number of cores per
    // node?
    // for now we just go random (except when createNodeSet and createNodeSet.shuffle=false are passed in)

    List<String> nodeList;

    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null)?null:StrUtils.splitSmart((CREATE_NODE_SET_EMPTY.equals(createNodeSetStr)?"":createNodeSetStr), ",", true);

    if (createNodeList != null) {
      nodeList = new ArrayList<>(createNodeList);
      nodeList.retainAll(liveNodes);
    } else {
      nodeList = new ArrayList<>(liveNodes);
    }

    Collections.shuffle(nodeList);

    return nodeList;
  }

  private void processResponses(NamedList results, ShardHandler shardHandler, Set<String> okayExceptions) {
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp, okayExceptions);
      }
    } while (srsp != null);
  }

  private void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError, String asyncId,
      Map<String,String> requestMap) {
    processResponses(results, shardHandler, abortOnError, msgOnError, asyncId, requestMap, Collections.<String>emptySet());
  }

  private void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError, String asyncId,
      Map<String,String> requestMap, Set<String> okayExceptions) {
    // Processes all shard responses
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp, okayExceptions);
        Throwable exception = srsp.getException();
        if (abortOnError && exception != null) {
          // drain pending requests
          while (srsp != null) {
            srsp = shardHandler.takeCompletedOrError();
          }
          throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError,
              exception);
        }
      }
    } while (srsp != null);

    // If request is async wait for the core admin to complete before returning
    if (asyncId != null) {
      waitForAsyncCallsToComplete(requestMap, results);
      requestMap.clear();
    }
  }

  private String createConfNode(String coll, ZkNodeProps message, boolean isLegacyCloud) throws KeeperException, InterruptedException {
    String configName = message.getStr(COLL_CONF);
    if(configName == null){
      // if there is only one conf, use that
      List<String> configNames=null;
      try {
        configNames = zkStateReader.getZkClient().getChildren(ZkController.CONFIGS_ZKNODE, null, true);
        if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          log.info("Only one config set found in zk - using it:" + configName);
        }
      } catch (KeeperException.NoNodeException e) {

      }

    }

    if(configName!= null){
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.info("creating collections conf node {} ", collDir);
      byte[] data = ZkStateReader.toJSON(ZkNodeProps.makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (zkStateReader.getZkClient().exists(collDir, true)) {
        zkStateReader.getZkClient().setData(collDir, data, true);
      } else {
        zkStateReader.getZkClient().makePath(collDir, data, true);
      }
    } else {
      if(isLegacyCloud){
        log.warn("Could not obtain config name");
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Unable to get config name");
      }
    }
    return configName;

  }

 private void collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params, NamedList results, String stateMatcher) {
   collectionCmd(clusterState, message, params, results, stateMatcher, Collections.<String>emptySet());
 }
  
 private void collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params, NamedList results, String stateMatcher, Set<String> okayExceptions) {
    log.info("Executing Collection Cmd : " + params);
    String collectionName = message.getStr("name");
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    
    DocCollection coll = clusterState.getCollection(collectionName);
    
    for (Map.Entry<String,Slice> entry : coll.getSlicesMap().entrySet()) {
      Slice slice = entry.getValue();
      sliceCmd(clusterState, params, stateMatcher, slice, shardHandler);
    }

    processResponses(results, shardHandler, okayExceptions);

  }

 private void sliceCmd(ClusterState clusterState, ModifiableSolrParams params,
     String stateMatcher, Slice slice, ShardHandler shardHandler,
     String asyncId, Map<String,String> requestMap) {

   for (Replica replica : slice.getReplicas()) {
     if (clusterState
         .liveNodesContain(replica.getStr(ZkStateReader.NODE_NAME_PROP))
         && (stateMatcher == null || stateMatcher.equals(replica.getState()))) {
       // For thread safety, only simple clone the ModifiableSolrParams
       ModifiableSolrParams cloneParams = new ModifiableSolrParams();
       cloneParams.add(params);
       cloneParams.set(CoreAdminParams.CORE,
           replica.getStr(ZkStateReader.CORE_NAME_PROP));

       sendShardRequest(replica.getStr(ZkStateReader.NODE_NAME_PROP),
           cloneParams, shardHandler, asyncId, requestMap);
     }
   }
 }

private void sliceCmd(ClusterState clusterState, ModifiableSolrParams params, String stateMatcher,
                        Slice slice, ShardHandler shardHandler) {
    Map<String,Replica> shards = slice.getReplicasMap();
    Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
    for (Map.Entry<String,Replica> shardEntry : shardEntries) {
      final ZkNodeProps node = shardEntry.getValue();
      if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP)) && (stateMatcher != null ? node.getStr(ZkStateReader.STATE_PROP).equals(stateMatcher) : true)) {
        // For thread safety, only simple clone the ModifiableSolrParams
        ModifiableSolrParams cloneParams = new ModifiableSolrParams();
        cloneParams.add(params);
        cloneParams.set(CoreAdminParams.CORE,
            node.getStr(ZkStateReader.CORE_NAME_PROP));

        String replica = node.getStr(ZkStateReader.BASE_URL_PROP);
        ShardRequest sreq = new ShardRequest();
        sreq.nodeName = node.getStr(ZkStateReader.NODE_NAME_PROP);
        // yes, they must use same admin handler path everywhere...
        cloneParams.set("qt", adminPath);
        sreq.purpose = 1;
        sreq.shards = new String[] {replica};
        sreq.actualShards = sreq.shards;
        sreq.params = cloneParams;
        log.info("Collection Admin sending CoreAdmin cmd to " + replica
            + " params:" + sreq.params);
        shardHandler.submit(sreq, replica, sreq.params);
      }
    }
  }
  
  private void processResponse(NamedList results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  private void processResponse(NamedList results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    if (e != null) {
      log.error("Error from shard: " + shard, e);

      SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
      if (failure == null) {
        failure = new SimpleOrderedMap();
        results.add("failure", failure);
      }

      failure.add(nodeName, e.getClass().getName() + ":" + e.getMessage());

    } else {

      SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
      if (success == null) {
        success = new SimpleOrderedMap();
        results.add("success", success);
      }

      success.add(nodeName, solrResponse.getResponse());
    }
  }

  private void waitForAsyncCallsToComplete(Map<String, String> requestMap, NamedList results) {
    for(String k:requestMap.keySet()) {
      log.debug("I am Waiting for :{}/{}", k, requestMap.get(k));
      results.add(requestMap.get(k), waitForCoreAdminAsyncCallToComplete(k, requestMap.get(k)));
    }
  }

  private NamedList waitForCoreAdminAsyncCallToComplete(String nodeName, String requestId) {
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList results = new NamedList();
          processResponse(results, srsp, Collections.<String>emptySet());
          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
              break;
            }
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
  }

  @Override
  public String getName() {
    return "Overseer Collection Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "collection_" + operation;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP) ?
      message.getStr(COLLECTION_PROP) : message.getStr("name");
  }

  @Override
  public void markExclusiveTask(String collectionName, ZkNodeProps message) {
    if (!CLUSTERSTATUS.isEqual(message.getStr(Overseer.QUEUE_OPERATION)) && collectionName != null) {
      synchronized (collectionWip) {
        collectionWip.add(collectionName);
      }
    }
  }

  @Override
  public void unmarkExclusiveTask(String collectionName, String operation, ZkNodeProps message) {
    if(!CLUSTERSTATUS.isEqual(operation) && collectionName != null) {
      synchronized (collectionWip) {
        collectionWip.remove(collectionName);
      }
    }
  }

  @Override
  public ExclusiveMarking checkExclusiveMarking(String collectionName, ZkNodeProps message) {
    // CLUSTERSTATUS is always mutually exclusive
    //TODO deprecated remove this check .
    if(CLUSTERSTATUS.isEqual(message.getStr(Overseer.QUEUE_OPERATION)))
      return ExclusiveMarking.EXCLUSIVE;

    synchronized (collectionWip) {
      if(collectionWip.contains(collectionName))
        return ExclusiveMarking.NONEXCLUSIVE;
    }

    return ExclusiveMarking.NOTDETERMINED;
  }
}
