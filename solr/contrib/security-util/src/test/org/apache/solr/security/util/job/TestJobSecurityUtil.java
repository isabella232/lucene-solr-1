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
package org.apache.solr.security.util.job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster;
import static org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster.USER_PARAM;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJobSecurityUtil extends SolrTestCaseJ4 {
  private static Logger log = LoggerFactory.getLogger(TestJobSecurityUtil.class);
  private static final int NUM_SERVERS = 2;
  private static MiniSolrCloudCluster miniCluster;
  private static HttpSolrServer userAuthServer;
  private static String baseURL;
  private static String zkHost;

  @BeforeClass
  public static void startup() throws Exception {
    File solrXml = SolrTestCaseJ4.getFile("solr/solr-no-core.xml");
    miniCluster = new HttpParamDelegationTokenMiniSolrCloudCluster(NUM_SERVERS, null,
      solrXml, null);
    JettySolrRunner runner = miniCluster.getJettySolrRunners().get(0);
    baseURL = runner.getBaseUrl().toString();
    userAuthServer = new UserAuthHttpSolrServer(baseURL);
    zkHost = miniCluster.getZkServer().getZkAddress();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    userAuthServer.shutdown();
  }

  @Test
  public void testCredentialsPass() throws Exception {
    // with jaas config
    Job job = new Job();
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try {
      // get a new UserAuthSolrServer for the jaas conf
      HttpSolrServer jaasConfServer = new UserAuthHttpSolrServer(baseURL);
      JobSecurityUtil.initCredentials(jaasConfServer, job, zkHost);
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
      verifyCredentialsPass(job);
    } finally {
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    }

    // no jaas conf, but enabled via configuration
    job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    verifyCredentialsPass(job);
  }

  @Test
  public void testCredentialsFail() throws Exception {
    // no jaas config, should fail
    Job job = new Job();
    JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    verifyCredentialsFail(job);

    // jaas config, but disabled via conf
    job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, false);
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try {
      JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    } finally {
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    }
  }

  @Test
  public void testBadArgs() throws Exception {
    // test null
    Job job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    try {
      JobSecurityUtil.initCredentials(userAuthServer, job, null);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
    try {
      JobSecurityUtil.loadCredentialsForClients(null, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
    try {
      JobSecurityUtil.cleanupCredentials(null, job, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
  }

  private void verifyCredentialsPass(Job job) throws Exception {
    // Ensure token is present in job credentials
    Token<? extends TokenIdentifier> authToken =
      job.getCredentials().getToken(new Text(zkHost));
    assertNotNull(authToken);
    JobSecurityUtil.loadCredentialsForClients(job, zkHost);
    String token = System.getProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
    assertNotNull(token);
    assertEquals(token, new String(authToken.getIdentifier(), "UTF-8"));

    HttpSolrServer server = new HttpSolrServer(baseURL);
    try {
      // Ensure a normal HttpSolrServer "just works"
      doSolrRequest(server, 200);

      // Cleanup credentials and verify server no longer works
      JobSecurityUtil.cleanupCredentials(server, job, zkHost);
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      System.clearProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
      server.shutdown();
    }

    // a new server shouldn't work
    server = new HttpSolrServer(baseURL);
    try {
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);
    } finally {
      server.shutdown();
    }

    // a new server shouldn't work even if we try to use the token
    server = new TokenAuthHttpSolrServer(baseURL, token);
    try {
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      server.shutdown();
    }
  }

  private void verifyCredentialsFail(Job job) throws Exception {
    // Ensure token is not present
    Token<? extends TokenIdentifier> authToken =
      job.getCredentials().getToken(new Text(zkHost));
    assertNull(authToken);
    JobSecurityUtil.loadCredentialsForClients(job, zkHost);
    String token = System.getProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
    assertNull(token);

    HttpSolrServer server = new HttpSolrServer(baseURL);
    try {
      // Ensure a normal HttpSolrServer doesn't work
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);

      JobSecurityUtil.cleanupCredentials(server, job, zkHost);
    } finally {
      server.shutdown();
    }
  }

  private void doSolrRequest(HttpSolrServer server, int expectedStatusCode)
  throws Exception {
    try {
      CoreAdminRequest request = new CoreAdminRequest() {
        @Override
        public SolrParams getParams() {
          return new ModifiableSolrParams();
        }
      };
      server.request(request);
      assertEquals(200, expectedStatusCode);
    } catch (HttpSolrServer.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }

  /**
   * HttpSolrServer that automatically authenticates via USER_PARAM
   */
  private static class UserAuthHttpSolrServer extends HttpSolrServer {
    public UserAuthHttpSolrServer(String baseURL) {
      super(baseURL);
      invariantParams = new ModifiableSolrParams();
      invariantParams.set(USER_PARAM, "user");
    }
  }

  /**
   * HttpSolrServer that automatically authenticates via the delegation token
   */
  private static class TokenAuthHttpSolrServer extends HttpSolrServer {
    private TokenAuthHttpSolrServer(String baseURL, String token) {
      super(baseURL);
      invariantParams = new ModifiableSolrParams();
      invariantParams.set(HttpSolrServer.DELEGATION_TOKEN_PARAM, token);
    }
  }
}
