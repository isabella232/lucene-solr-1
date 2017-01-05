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

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_SECURITY_CONF_PATH;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CLOUDERA BUILD.
 * A unit test to verify ZkSyncTool functionality.
 */
public final class ZkSyncToolTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ZkTestServer zkServer;
  private SolrZkClient zkClient;
  private String zkDir;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    log.info("####SETUP_START " + getTestName());

    File tmpDir = createTempDir().toFile();

    zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    System.setProperty("zkHost", zkServer.getZkAddress());

    try (SolrZkClient client = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT)) {
      client.makePath("/path1", false, true);
      client.makePath("/path1/path2", false, true);
    }

    zkClient = new SolrZkClient(zkServer.getZkAddress("/path1/path2"), AbstractZkTestCase.TIMEOUT);

    log.info("####SETUP_END " + getTestName());
  }

  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    zkServer.shutdown();
    super.tearDown();
  }

  public void testConfigureSecurity() throws Exception {
    Path configFilePath = TEST_PATH().resolve("security").resolve("security.json");
    Path proxyUserConfigFilePath = TEST_PATH().resolve("security").resolve("security_with_proxy_users.json");

    // Configure security without proxy users.
    Map<String, String> env = new HashMap<>();
    env.put("SOLR_ZK_ENSEMBLE", zkServer.getZkAddress("/path1/path2"));
    env.put("SOLR_SEC_CONFIG_FILE", configFilePath.toString());
    env.put("SOLR_AUTHENTICATION_TYPE", "kerberos");

    ZkSyncTool tool = new ZkSyncTool(env);
    tool.sync();

    assertTrue(zkClient.exists(SOLR_SECURITY_CONF_PATH, true));

    // Build the expected security config.
    Map<String, Object> securityConfig = (Map<String, Object>)readFromFileSystem(configFilePath);
    Map<String, Object> authConfig = (Map<String, Object>)securityConfig.get("authentication");
    Map<String, String> proxyUserConfigs = (Map<String, String>)authConfig.getOrDefault("proxyUserConfigs", new HashMap<>());
    proxyUserConfigs.put("proxyuser.solr.groups", "*");
    proxyUserConfigs.put("proxyuser.solr.hosts", "*");
    authConfig.put("proxyUserConfigs", proxyUserConfigs);
    securityConfig.put("authentication", authConfig);

    assertTrue(Objects.deepEquals(securityConfig, readConfigFromZk(SOLR_SECURITY_CONF_PATH)));

    // Now configure some proxy users.
    env.put("SOLR_SECURITY_ALLOWED_PROXYUSERS", "hue");
    env.put("SOLR_SECURITY_PROXYUSER_hue_HOSTS", "*");
    env.put("SOLR_SECURITY_PROXYUSER_hue_GROUPS", "*");

    tool.sync();

    assertTrue(zkClient.exists(SOLR_SECURITY_CONF_PATH, true));
    assertTrue(Objects.deepEquals(readFromFileSystem(proxyUserConfigFilePath), readConfigFromZk(SOLR_SECURITY_CONF_PATH)));

    // Disable security
    env.remove("SOLR_AUTHENTICATION_TYPE");
    tool.sync();

    assertFalse(zkClient.exists(SOLR_SECURITY_CONF_PATH, true));
  }

  private Object readConfigFromZk(String zkPath) throws KeeperException, InterruptedException {
    return Utils.fromJSON(zkClient.getData(zkPath, null, new Stat(), true));
  }

  private Object readFromFileSystem(Path filePath) throws IOException {
    return Utils.fromJSON(Files.readAllBytes(filePath));
  }
}
