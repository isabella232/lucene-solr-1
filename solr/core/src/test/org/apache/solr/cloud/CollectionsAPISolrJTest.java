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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;

import java.io.File;
import java.util.Map;
import java.util.Properties;

@LuceneTestCase.Slow
public class CollectionsAPISolrJTest extends AbstractFullDistribZkTestBase {

  @Override
  public void doTest() throws Exception {
    testCreateCollectionWithPropertyParam();
  }

  public void tearDown() throws Exception {
    if (controlClient != null) {
      controlClient.shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();
  }


  private void testCreateCollectionWithPropertyParam() throws Exception {
    String collectionName = "solrj_test_core_props";

    File tmpDir = createTempDir("testPropertyParamsForCreate");
    File instanceDir = new File(tmpDir, "instanceDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File dataDir = new File(tmpDir, "dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    File ulogDir = new File(tmpDir, "ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));

    Properties properties = new Properties();
    properties.put(CoreAdminParams.INSTANCE_DIR, instanceDir.getAbsolutePath());
    properties.put(CoreAdminParams.DATA_DIR, dataDir.getAbsolutePath());
    properties.put(CoreAdminParams.ULOG_DIR, ulogDir.getAbsolutePath());

    CollectionAdminRequest.Create createReq = new CollectionAdminRequest.Create();
    createReq.setCollectionName(collectionName);
    createReq.setNumShards(1);
    createReq.setConfigName("conf1");
    createReq.setProperties(properties);

    CollectionAdminResponse response = createReq.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

    DocCollection testCollection = cloudClient.getZkStateReader()
        .getClusterState().getCollection(collectionName);

    Replica replica1 = testCollection.getReplica("core_node1");

    HttpSolrServer solrServer = new HttpSolrServer(replica1.getStr("base_url"));
    try {
      CoreAdminResponse status = CoreAdminRequest.getStatus(replica1.getStr("core"), solrServer);
      NamedList<Object> coreStatus = status.getCoreStatus(replica1.getStr("core"));
      String dataDirStr = (String) coreStatus.get("dataDir");
      String instanceDirStr = (String) coreStatus.get("instanceDir");
      assertEquals("Instance dir does not match param passed in property.instanceDir syntax",
          new File(instanceDirStr).getAbsolutePath(), instanceDir.getAbsolutePath());
      assertEquals("Data dir does not match param given in property.dataDir syntax",
          new File(dataDirStr).getAbsolutePath(), dataDir.getAbsolutePath());

    } finally {
      solrServer.shutdown();
    }

    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName(collectionName);
    deleteCollectionRequest.process(cloudClient);
  }
}