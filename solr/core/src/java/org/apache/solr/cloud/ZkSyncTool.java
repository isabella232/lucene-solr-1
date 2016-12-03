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

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.ConfigurableInternodeAuthHadoopPlugin;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLOUDERA BUILD
 * This class implements a command-line tool to synchronize the Solr configuration in Zookeeper
 * by reading the relevant environment variables. This is specific to CDH environment only.
 *
 * This tool currently configures security for Solr (i.e. security.json file) in Zookeeper. It reads the
 * basic configuration from the file located by SOLR_SEC_CONFIG_FILE environment variable. This tool
 * works only with Hadoop authentication plugin for Solr (specifically
 * {@linkplain ConfigurableInternodeAuthHadoopPlugin}) although this can be improved in future.
 */
public class ZkSyncTool {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Map<String,String> env;

  public ZkSyncTool(Map<String,String> env) {
    this.env = env;
  }

  public static void main(String[] args) {
    ZkSyncTool tool = new ZkSyncTool(System.getenv());
    try {
      tool.sync();
      System.exit(0);
    } catch (Exception e) {
      log.error("Unexpected exception " + e.getLocalizedMessage(), e);
      System.exit(1);
    }
  }

  /**
   * This method synchronizes the Solr configuration based on the values of the
   * provided environment variables. Currently it handles following configurations,
   * - security.json for configuring authentication and authorization.
   *
   * @throws Exception in case of error.
   */
  public void sync() throws Exception {
    String solrZkEnsemble = Objects.requireNonNull(env.get("SOLR_ZK_ENSEMBLE"),
        "Please specify SOLR_ZK_ENSEMBLE environment variable");
    String solrSecConfigFile = Objects.requireNonNull(env.get("SOLR_SEC_CONFIG_FILE"),
        "Please specify SOLR_SEC_CONFIG_FILE environment variable");

    // If ACLs are enabled for Zookeeper, configure the security for zk client.
    String secureZk = env.get("SECURE_ZNODE");
    if (secureZk != null) {
      String jaasConf = Objects.requireNonNull(env.get("SOLR_AUTHENTICATION_JAAS_CONF"),
          "Please specify SOLR_AUTHENTICATION_JAAS_CONF environment variable");

      System.setProperty("zkACLProvider", "org.apache.solr.common.cloud.SaslZkACLProvider");
      System.setProperty("java.security.auth.login.config", jaasConf);
    }

    try (SolrZkClient zkClient = new SolrZkClient(solrZkEnsemble, 30000, 30000)) {
      String authType = env.get("SOLR_AUTHENTICATION_TYPE");
      if (authType != null) { // security enabled.
        Path securityJsonPath = Paths.get(solrSecConfigFile);
        byte[] securityConf = Files.readAllBytes(securityJsonPath);
        Map securityConfigMap = (Map) Utils.fromJSON(securityConf);
        // Perform extra processing.
        prepareSecurityJson(securityConfigMap);
        updateConfig(zkClient, ZkStateReader.SOLR_SECURITY_CONF_PATH, securityConfigMap);

      } else { // security disabled. Remove security config if present.
        deleteConfig(zkClient, ZkStateReader.SOLR_SECURITY_CONF_PATH);
      }
    }
  }

  /**
   * This method prepares the configuration of security.json file by reading the relevant
   * environment variables.
   *
   * @param securityConfig The default configuration of security.json (which is to be overriden).
   */
  @SuppressWarnings("unchecked")
  private void prepareSecurityJson(Map securityConfig) {
    Map<String, Object> authConfig = (Map<String, Object>)securityConfig.get("authentication");
    Map<String, String> authConfigDefaults = (Map<String, String>)authConfig.getOrDefault("defaultConfigs", new HashMap<>());
    Map<String, String> proxyUserConfigs = (Map<String, String>)authConfig.getOrDefault("proxyUserConfigs", new HashMap<>());

    // Configure proxy users support
    String proxyUsersStr = env.getOrDefault("SOLR_SECURITY_ALLOWED_PROXYUSERS", "");
    String[] proxyUsers = proxyUsersStr.split(",");
    for (String proxyUser : proxyUsers) {
      String hostsConfig = env.get("SOLR_SECURITY_PROXYUSER_"+proxyUser+"_HOSTS");
      if (hostsConfig != null) {
        proxyUserConfigs.put("proxyuser."+proxyUser+".hosts", hostsConfig);
      }

      String groupsConfig = env.get("SOLR_SECURITY_PROXYUSER_"+proxyUser+"_GROUPS");
      if (groupsConfig != null) {
        proxyUserConfigs.put("proxyuser."+proxyUser+".groups", groupsConfig);
      }
    }

    // Solr superuser must be able to proxy any user in order to properly forward requests
    // Don't grant proxy privileges to superUser automatically since user groups or user hosts
    // may already be set for superUser
    String superUser = env.getOrDefault("SOLR_AUTHORIZATION_SUPERUSER", "solr");
    if (!proxyUserConfigs.containsKey("proxyuser."+superUser+".groups") &&
        !proxyUserConfigs.containsKey("proxyuser."+superUser+".hosts")) {
      proxyUserConfigs.put("proxyuser."+superUser+".groups", "*");
      proxyUserConfigs.put("proxyuser."+superUser+".hosts", "*");
    }

    // Configure delegation token support
    String solrZkEnsemble = env.get("SOLR_ZK_ENSEMBLE");
    String[] zkPath = solrZkEnsemble.split("/");
    if (zkPath != null && zkPath.length > 1) {
      authConfigDefaults.put("zk-dt-secret-manager.znodeWorkingPath", zkPath[1].concat("/security/zkdtsm"));
    }

    authConfig.put("proxyUserConfigs", proxyUserConfigs);
    authConfig.put("defaultConfigs", authConfigDefaults);
  }

  /**
   * This method updates the specified Zookeeper path with the JSON representation of
   * proposed configuration value in an idempotent fashion.
   *
   * @param zkClient The Solr Zookeeper client
   * @param zkPath The Zookeeper path storing the proposed configuration
   * @param proposedConfig The proposed configuration object.
   * @throws KeeperException In case of Zookeeper error
   * @throws InterruptedException In case of interruption.
   */
  private void updateConfig(SolrZkClient zkClient, String zkPath, Object proposedConfig)
      throws KeeperException, InterruptedException {
    boolean done = false;

    while (!done) {
      Stat s = new Stat();

      try {
        if (zkClient.exists(zkPath, true)) {
          Object currentConfig = Utils.fromJSON(zkClient.getData(zkPath, null, s, true));
          if (!Objects.deepEquals(currentConfig, proposedConfig)) {
            log.info(
                "The current configuration at ZK path {} is different than the proposed configuration. Sync is necessary.",
                zkPath);
            zkClient.setData(zkPath, Utils.toJSON(proposedConfig), s.getVersion(), true);
          }
        } else {
          log.info("Configuration missing at path {}. Sync is necessary.", zkPath);
          zkClient.create(zkPath, Utils.toJSON(proposedConfig), CreateMode.PERSISTENT, true);
        }

        done = true;

      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        // race condition. continue.
        log.info("Unable to sync the configuration to Zk due to {}. Retrying...", e.getLocalizedMessage());
      }
    }
  }

  /**
   * This method deletes the configuration information stored in the specified Zookeeper path in an idempotent
   * fashion.
   *
   * @param zkClient The Solr Zookeeper client
   * @param zkPath The Zookeeper path storing the proposed configuration
   * @throws KeeperException  In case of Zookeeper error
   * @throws InterruptedException In case of interruption.
   */
  private void deleteConfig(SolrZkClient zkClient, String zkPath) throws KeeperException, InterruptedException {
    while (zkClient.exists(zkPath, true)) {
      try {
        zkClient.delete(zkPath, -1, true);
        log.info("Configuration at ZK path {} is deleted.", zkPath);
      } catch (KeeperException.NoNodeException e) {
        // race condition.
      }
    }
  }
}
