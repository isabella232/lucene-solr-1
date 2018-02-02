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

package org.apache.solr.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.solr.config.upgrade.ConfigParserTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ConfigParserToolTest {

  public static final String COLLECTION3_PART = "{\"collection3\":{\"routerSpec\":{\"name\":\"compositeId\"},\"replicationFactor\":\"2\",\"shards\":{\"shard1\":{\"range\":\"80000000-d554ffff\",\"state\":\"active\",\"replicas\":{\"core_node1\":{\"core\":\"collection3_shard1_replica1\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\",\"leader\":\"true\"},\"core_node2\":{\"core\":\"collection3_shard1_replica2\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\"}}},\"shard2\":{\"range\":\"d5550000-2aa9ffff\",\"state\":\"active\",\"replicas\":{\"core_node3\":{\"core\":\"collection3_shard2_replica1\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\",\"leader\":\"true\"},\"core_node4\":{\"core\":\"collection3_shard2_replica2\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\"}}},\"shard3\":{\"range\":\"2aaa0000-7fffffff\",\"state\":\"active\",\"replicas\":{\"core_node5\":{\"core\":\"collection3_shard3_replica1\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\"},\"core_node6\":{\"core\":\"collection3_shard3_replica2\",\"base_url\":\"http://mano-c5x.gce.cloudera.com:8983/solr\",\"node_name\":\"mano-c5x.gce.cloudera.com:8983_solr\",\"state\":\"active\",\"leader\":\"true\"}}}},\"router\":\"compositeId\",\"maxShardsPerNode\":\"6\",\"autoAddReplicas\":\"false\"}}\n";
  private ByteArrayOutputStream output;
  private Integer lastExitCode;

  @Before
  public void setUp() throws UnsupportedEncodingException {
    output = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(output, false, StandardCharsets.UTF_8.name());
    ConfigParserTool.setOut(ps);
    ConfigParserTool.setExitFunction((i) -> lastExitCode = i);
    lastExitCode = null;
  }

  @After
  public void tearDown() throws Exception {
    ConfigParserTool.setOut(System.out);
    ConfigParserTool.setExitFunction(System::exit);
  }

  @Test
  public void testPrintCollectionState() {
    String clusterstateJson = ConfigParserToolTest.class.getResource("/zksave/clusterstate.json").getPath();
    ConfigParserTool.main(new String[]{"--" + ConfigParserTool.GET_COLLECTION_STATE_COMMAND, "-i", clusterstateJson, "-c", "collection3"});
    assertTrue(new String(output.toByteArray(), StandardCharsets.UTF_8).equals(COLLECTION3_PART));
    assertTrue(0 == lastExitCode);
  }
}
