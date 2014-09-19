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

package org.apache.solr.cloud.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.cloud.ChaosMonkey;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@Slow
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class BasicHdfsTest extends BasicDistributedZkTest {
  private static final String DELETE_DATA_DIR_COLLECTION = "delete_data_dir";
  
  private static MiniDFSCluster dfsCluster;
  
  private boolean testRestartIntoSafeMode;
  
  @BeforeClass
  public static void setupClass() throws Exception {

    dfsCluster = HdfsTestUtil.setupClass(new File(TEMP_DIR,
        HdfsBasicDistributedZk2Test.class.getName() + "_"
            + System.currentTimeMillis()).getAbsolutePath());
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    dfsCluster = null;
  }

  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
  
  public BasicHdfsTest() {
    super();
    sliceCount = 1;
    shardCount = 1;
    testRestartIntoSafeMode = random().nextBoolean();
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  @Override
  public void doTest() throws Exception {
    createCollection(DELETE_DATA_DIR_COLLECTION, 1, 1, 1);
    waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
    cloudClient.setDefaultCollection(DELETE_DATA_DIR_COLLECTION);
    cloudClient.getZkStateReader().updateClusterState(true);
    NamedList<Object> response = cloudClient.query(
        new SolrQuery().setRequestHandler("/admin/system")).getResponse();
    NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
    String dataDir = (String) ((NamedList<Object>) coreInfo.get("directory"))
        .get("data");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", DELETE_DATA_DIR_COLLECTION);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
    
    Configuration conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    FileSystem fs = FileSystem.get(new URI(dataDir), conf);
    assertFalse(
        "Data directory exists after collection removal : "
            + dataDir, fs.exists(new Path(dataDir)));
    fs.close();
    
    if (testRestartIntoSafeMode) {
      createCollection(DELETE_DATA_DIR_COLLECTION, 1, 1, 1);
      
      waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
      
      ChaosMonkey.stop(jettys.get(0));
      
      // enter safe mode and restart a node
      NameNodeAdapter.enterSafeMode(dfsCluster.getNameNode(), false);
      
      int rnd = LuceneTestCase.random().nextInt(10000);
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        
        @Override
        public void run() {
          NameNodeAdapter.leaveSafeMode(dfsCluster.getNameNode());
        }
      }, rnd);
      
      ChaosMonkey.start(jettys.get(0));
      
      waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
    }
  }
}
