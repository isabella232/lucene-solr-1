
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.servlet;

import org.apache.commons.io.IOUtils;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import static org.apache.solr.servlet.SolrHadoopAuthenticationFilter.SOLR_PROXYUSER_PREFIX;
import static org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster.USER_PARAM;
import static org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster.REMOTE_HOST_PARAM;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the delegation token support in the {@link SolrHadoopAuthenticationFilter}.
 */
public class SolrHadoopAuthenticationFilterDelegationTokenTest extends SolrTestCaseJ4 {
  private static Logger log = LoggerFactory.getLogger(SolrHadoopAuthenticationFilterDelegationTokenTest.class);
  private static final int NUM_SERVERS = 2;
  private static HttpParamDelegationTokenMiniSolrCloudCluster miniCluster;
  private static HttpSolrServer solrServer;

  @BeforeClass
  public static void startup() throws Exception {
    String testHome = SolrTestCaseJ4.TEST_HOME();
    miniCluster = new HttpParamDelegationTokenMiniSolrCloudCluster(NUM_SERVERS, null,
      new File(testHome, "solr-no-core.xml"), null, sslConfig);
    JettySolrRunner runner = miniCluster.getJettySolrRunners().get(0);
    solrServer = new HttpSolrServer(runner.getBaseUrl().toString());
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    solrServer.shutdown();
    solrServer = null;
  }

  private SolrRequest getProxyRequest(String user, String doAs, String remoteHost) {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(USER_PARAM, user);
    params.set("doAs", doAs);
    if (remoteHost != null) params.set(REMOTE_HOST_PARAM, remoteHost);
    return new CoreAdminRequest() {
      @Override
      public SolrParams getParams() {
        return params;
      }
    };
  }

  private String getExpectedGroupExMsg(String user, String doAs) {
    return "User: " + user + " is not allowed to impersonate " + doAs;
  }

  private String getExpectedHostExMsg(String user) {
    return "Unauthorized connection for super-user: " + user;
  }

  private String getTokenQueryString(String baseURL, String user, String op,
      String delegation, String token, String renewer) {
    StringBuilder builder = new StringBuilder();
    builder.append(baseURL).append("/admin/cores?");
    if (user != null) {
      builder.append(USER_PARAM).append("=").append(user).append("&");
    }
    builder.append("op=").append(op);
    if (delegation != null) {
      builder.append("&delegation=").append(delegation);
    }
    if (token != null) {
      builder.append("&token=").append(token);
    }
    if (renewer != null) {
      builder.append("&renewer=").append(renewer);
    }
    return builder.toString();
  }

  private HttpResponse getHttpResponse(HttpUriRequest request) throws Exception {
    HttpClient httpClient = solrServer.getHttpClient();
    HttpResponse response = null;
    boolean success = false;
    try {
      response = httpClient.execute(request);
      success = true;
    } finally {
      if (!success) {
        request.abort();
      }
    }
    return response;
  }

  private String getDelegationToken(final String renewer, final String user) throws Exception {
    DelegationTokenRequest.Get get = new DelegationTokenRequest.Get(renewer) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(USER_PARAM, user);
        return params;
      }
    };
    DelegationTokenResponse.Get getResponse = get.process(solrServer);
    return getResponse.getDelegationToken();
  }

  private long renewDelegationToken(final String token ,final int expectedStatusCode,
      final String user) throws Exception {
    return renewDelegationToken(token, expectedStatusCode, user, solrServer.getBaseURL());
  }

  private long renewDelegationToken(final String token, final int expectedStatusCode,
      final String user, final String url) throws Exception {
    DelegationTokenRequest.Renew renew = new DelegationTokenRequest.Renew(token) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(USER_PARAM, user);
        return params;
      }

      @Override
      public Set<String> getQueryParams() {
        Set<String> queryParams = super.getQueryParams();
        queryParams.add(USER_PARAM);
        return queryParams;
      }
    };
    HttpSolrServer server = new HttpSolrServer(url);
    try {
      DelegationTokenResponse.Renew renewResponse = renew.process(server);
      assertEquals(200, expectedStatusCode);
      return renewResponse.getExpirationTime();
    } catch (HttpSolrServer.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
      return -1;
    } finally {
      server.shutdown();
    }
  }

  private void cancelDelegationToken(String token, int expectedStatusCode)
  throws Exception {
    cancelDelegationToken(token, expectedStatusCode, solrServer.getBaseURL());
  }

  private void cancelDelegationToken(String token, int expectedStatusCode, String url)
  throws Exception {
    DelegationTokenRequest.Cancel cancel = new DelegationTokenRequest.Cancel(token);
    HttpSolrServer server = new HttpSolrServer(url);
    try {
      DelegationTokenResponse.Cancel cancelResponse = cancel.process(server);
      assertEquals(200, expectedStatusCode);
    } catch (HttpSolrServer.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    } finally {
      server.shutdown();
    }
  }

  private void doSolrRequest(String token, int expectedStatusCode)
  throws Exception {
    doSolrRequest(token, expectedStatusCode, solrServer.getBaseURL());
  }

  private void doSolrRequest(String token, int expectedStatusCode, String url)
  throws Exception {
    HttpGet get = new HttpGet(getTokenQueryString(
      url, null, "op", token, null, null));
    HttpResponse response = getHttpResponse(get);
    assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
    EntityUtils.consumeQuietly(response.getEntity());
  }

  private void doSolrRequest(HttpSolrServer server, SolrRequest request,
      int expectedStatusCode) throws Exception {
    try {
      server.request(request);
      assertEquals(200, expectedStatusCode);
    } catch (HttpSolrServer.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }

  private void verifyTokenValid(String token) throws Exception {
     // pass with token
    doSolrRequest(token, 200);

    // fail without token
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code);

    // pass with token on other server
    String otherServerUrl =
      miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();
    doSolrRequest(token, 200, otherServerUrl);

    // fail without token on other server
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, otherServerUrl);
  }

  /**
   * Test basic Delegation Token get/verify
   */
  @Test
  public void testDelegationTokenVerify() throws Exception {
    final String user = "bar";

    // Get token
    String token = getDelegationToken(null, user);
    assertNotNull(token);

    verifyTokenValid(token);
  }

  private void verifyTokenCancelled(String token) throws Exception {
    String otherServerUrl =
      miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();

    // fail with token on both servers
    doSolrRequest(token, ErrorCode.FORBIDDEN.code);
    doSolrRequest(token, ErrorCode.FORBIDDEN.code, otherServerUrl);

    // fail without token on both servers
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code);
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code, otherServerUrl);
  }

  @Test
  public void testDelegationTokenCancel() throws Exception {
    {
      // Get token
      String token = getDelegationToken(null, "user");
      assertNotNull(token);

      // cancel token, note don't need to be authenticated to cancel (no user specified)
      cancelDelegationToken(token, 200);
      verifyTokenCancelled(token);
    }

    {
      // cancel token on different server from where we got it
      String token = getDelegationToken(null, "user");
      assertNotNull(token);

      String otherServerUrl =
        miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();
      cancelDelegationToken(token, 200, otherServerUrl);
      verifyTokenCancelled(token);
    }
  }

  @Test
  public void testDelegationTokenCancelFail() throws Exception {
    // cancel a bogu token
    cancelDelegationToken("BOGUS", ErrorCode.NOT_FOUND.code);

    String otherServerUrl =
      miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();
    {
      // cancel twice, first on same server
      String token = getDelegationToken(null, "bar");
      assertNotNull(token);
      cancelDelegationToken(token, 200);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, otherServerUrl);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code);
    }

    {
      // cancel twice, first on other server
      String token = getDelegationToken(null, "bar");
      assertNotNull(token);
      cancelDelegationToken(token, 200, otherServerUrl);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code, otherServerUrl);
      cancelDelegationToken(token, ErrorCode.NOT_FOUND.code);
    }
  }

  private void verifyDelegationTokenRenew(String renewer, String user)
  throws Exception {
    {
      // renew on same server
      String token = getDelegationToken(renewer, user);
      assertNotNull(token);
      long currentTimeMillis = System.currentTimeMillis();
      assertTrue(renewDelegationToken(token, 200, user) > currentTimeMillis);
      verifyTokenValid(token);
    }

    {
      String otherServerUrl =
        miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();
      // renew on different server
      String token = getDelegationToken(renewer, user);
      assertNotNull(token);
      long currentTimeMillis = System.currentTimeMillis();
      assertTrue(renewDelegationToken(token, 200, user, otherServerUrl) > currentTimeMillis);
      verifyTokenValid(token);
    }
  }

  @Test
  public void testDelegationTokenRenew() throws Exception {
    // test with specifying renewer
    verifyDelegationTokenRenew("bar", "bar");

    // test without specify renewer
    verifyDelegationTokenRenew(null, "bar");
  }

  @Test
  public void testDelegationTokenRenewFail() throws Exception {
    String otherServerUrl =
      miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();

    // don't set renewer and try to renew as an a different user
    String token = getDelegationToken(null, "bar");
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "foo");
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "foo", otherServerUrl);

    // set renewer and try to renew as different user
    token = getDelegationToken("renewUser", "bar");
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser");
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser", otherServerUrl);
  }

  /**
   * Test that a non-delegation-token operation is handled correctly
   */
  @Test
  public void testDelegationOtherOp() throws Exception {
    HttpGet get = new HttpGet(getTokenQueryString(
      solrServer.getBaseURL(), "bar", "someSolrOperation", null, null, null));
    HttpResponse response = getHttpResponse(get);
    byte [] body = IOUtils.toByteArray(response.getEntity().getContent());
    assertTrue(new String(body, "UTF-8").contains("<int name=\"status\">0</int>"));
    EntityUtils.consumeQuietly(response.getEntity());
  }

  private SolrRequest getAdminCoreRequest(final SolrParams params) {
    return new SolrRequest(SolrRequest.METHOD.GET, "/admin/cores") {
      @Override
      public Collection<ContentStream> getContentStreams() {
        return null;
      }

      @Override
      public SolrParams getParams() {
        return params;
      }

      @Override
      public SolrResponse process(SolrServer server) {
        return null;
      }
    };
  }

  /**
   * Test HttpSolrServer's delegation token support
   */
  @Test
  public void testDelegationTokenSystemProperty() throws Exception {
    // Get token
    String token = getDelegationToken(null, "bar");
    assertNotNull(token);

    SolrRequest request = getAdminCoreRequest(new ModifiableSolrParams());
    JettySolrRunner runner = miniCluster.getJettySolrRunners().get(0);

    // test without token
    HttpSolrServer ss = new HttpSolrServer(runner.getBaseUrl().toString());
    doSolrRequest(ss, request, ErrorCode.UNAUTHORIZED.code);
    ss.shutdown();

    System.setProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY, token);
    try {
      ss = new HttpSolrServer(runner.getBaseUrl().toString());
      // test with token via property
      doSolrRequest(ss, request, 200);

      // test with param -- param should take precendence over system prop
      ModifiableSolrParams tokenParam = new ModifiableSolrParams();
      tokenParam.set(HttpSolrServer.DELEGATION_TOKEN_PARAM, "invalidToken");
      doSolrRequest(ss, getAdminCoreRequest(tokenParam), ErrorCode.FORBIDDEN.code);
      ss.shutdown();
    } finally {
      System.clearProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
    }
  }
}
