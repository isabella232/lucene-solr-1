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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@Nightly
public class LeaderInitiatedRecoveryOnShardRestartTest extends AbstractFullDistribZkTestBase {
  
  protected static final transient Logger log = LoggerFactory.getLogger(LeaderInitiatedRecoveryOnShardRestartTest.class);
  

  public LeaderInitiatedRecoveryOnShardRestartTest() {
    super();
    sliceCount = 1;
    shardCount = 3;
  }
  
  public void restartWithAllInLIRTest() throws Exception {
    waitForThingsToLevelOut(30000);

    String shardId = "shard1";

    Map<String, Object> stateObj = new HashMap<String,Object>();
    stateObj.put(ZkStateReader.STATE_PROP, "down");
    stateObj.put("createdByNodeName", "test");
    stateObj.put("createdByCoreNodeName", "test");
    
    byte[] znodeData = ZkStateReader.toJSON(stateObj);
    
    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node1", znodeData, true);
    zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node2", znodeData, true);
    zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node3", znodeData, true);

    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.stop(jetty);
    }
    
    Thread.sleep(2000);
    
    for (JettySolrRunner jetty : jettys) {
      ChaosMonkey.start(jetty);
    }
    
    // recoveries will not finish without SOLR-8075
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, true);

    // now expire each node
    try {
      zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node1", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    try {
      zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node2", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    try {
      zkClient.makePath("/collections/" + DEFAULT_COLLECTION + "/leader_initiated_recovery/" + shardId + "/core_node3", znodeData, true);
    } catch (NodeExistsException e) {
    
    }
    
    for (JettySolrRunner jetty : jettys) {
      chaosMonkey.expireSession(jetty);
    }
    
    Thread.sleep(2000);
    
    // recoveries will not finish without SOLR-8075
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, true);
  }

  @Override
  public void doTest() throws Exception {
    restartWithAllInLIRTest();
  }
}
