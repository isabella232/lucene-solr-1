package org.apache.solr.index.hdfs;

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.index.BaseTestCheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class CheckHdfsIndexTest extends AbstractFullDistribZkTestBase {
  private static MiniDFSCluster dfsCluster;
  private static Path path;

  private BaseTestCheckIndex testCheckIndex;
  private Directory directory;

  public CheckHdfsIndexTest() {
    super();
    sliceCount = 1;
    shardCount = 1;
    fixShardCount = true;

    testCheckIndex = new BaseTestCheckIndex();
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().getAbsolutePath());
    path = new Path(HdfsTestUtil.getURI(dfsCluster) + "/solr/");
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    directory = new HdfsDirectory(path, conf);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    directory.close();
    dfsCluster.getFileSystem().delete(path, true);
    super.tearDown();
  }

  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }

  public void doTest() throws Exception {
    indexr(id, 1);
    commit();

    waitForRecoveriesToFinish(false);

    String[] args;
    {
      SolrServer client = clients.get(0);
      NamedList<Object> response = client.query(new SolrQuery().setRequestHandler("/admin/system")).getResponse();
      NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
      String indexDir = (String) ((NamedList<Object>) coreInfo.get("directory")).get("data") + "/index";

      args = new String[] {indexDir};
    }

    assertEquals("CheckHdfsIndex return status", 0, CheckHdfsIndex.doMain(args));
  }

  @Test
  public void testDeletedDocs() throws IOException {
    testCheckIndex.testDeletedDocs(directory);
  }

  @Test
  public void testBogusTermVectors() throws IOException {
    testCheckIndex.testBogusTermVectors(directory);
  }

  @Test
  public void testChecksumsOnly() throws IOException {
    testCheckIndex.testChecksumsOnly(directory);
  }

  @Test
  public void testChecksumsOnlyVerbose() throws IOException {
    testCheckIndex.testChecksumsOnlyVerbose(directory);
  }
}
