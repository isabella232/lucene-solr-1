
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.util.EntityUtils;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import static org.apache.solr.servlet.SolrHadoopAuthenticationFilter.SOLR_PROXYUSER_PREFIX;

import org.codehaus.jackson.map.ObjectMapper;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the proxy user support in the {@link SolrHadoopAuthenticationFilter}.
 */
public class SolrHadoopAuthenticationFilterMiniClusterTest extends SolrTestCaseJ4 {
  private static Logger log = LoggerFactory.getLogger(SolrHadoopAuthenticationFilterMiniClusterTest.class);
  private static final int NUM_SERVERS = 2;
  private static MiniSolrCloudCluster miniCluster;
  private static HttpSolrServer solrServer;

  private static String getHttpParam(HttpServletRequest request, String param) {
    List<NameValuePair> pairs =
      URLEncodedUtils.parse(request.getQueryString(), Charset.forName("UTF-8"));
    for (NameValuePair nvp : pairs) {
      if(param.equals(nvp.getName())) {
        return nvp.getValue();
      }
    }
    return null;
  }

  public static class HttpParamAuthenticationHandler
      implements AuthenticationHandler {

    @Override
    public String getType() {
      return "dummy";
    }

    @Override
    public void init(Properties config) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException, AuthenticationException {
      return false;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
        HttpServletResponse response)
        throws IOException, AuthenticationException {
      AuthenticationToken token = null;
      String userName = getHttpParam(request, HttpParamToRequestFilter.USER);
      if (userName != null) {
        return new AuthenticationToken(userName, userName, "test");
      } else {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", "dummy");
      }
      return token;
    }
  }

  public static class HttpParamDelegationTokenAuthenticationHandler extends
      DelegationTokenAuthenticationHandler {
    public HttpParamDelegationTokenAuthenticationHandler() {
      super(new HttpParamAuthenticationHandler());
    }

    @Override
    public void init(Properties config) throws ServletException {
      Properties conf = new Properties(config);
      conf.setProperty(TOKEN_KIND, "token-kind");
      initTokenManager(conf);
    }
  }

  /**
   * Filter that converts http params to HttpServletRequest params
   */
  public static class HttpParamToRequestFilter extends SolrHadoopAuthenticationFilter {
    public static final String USER = "user";
    public static final String REMOTE_HOST = "remoteHost";

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = super.getConfiguration(configPrefix, filterConfig);
      conf.setProperty(AUTH_TYPE,
          HttpParamDelegationTokenAuthenticationHandler.class.getName());
      return conf;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
      final HttpServletRequest httpRequest = (HttpServletRequest)request;
      final HttpServletRequestWrapper requestWrapper = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getRemoteHost() {
          String param = getHttpParam(httpRequest, REMOTE_HOST);
          return param != null ? param : httpRequest.getRemoteHost();
        }
      };

      super.doFilter(requestWrapper, (HttpServletResponse)response, chain);
    }
  }

  private static String getGroup() throws Exception {
    org.apache.hadoop.security.Groups hGroups =
      new org.apache.hadoop.security.Groups(new Configuration());
    List<String> g = hGroups.getGroups(System.getProperty("user.name"));
    return g.get(0);
  }

  private static Map<String, String> getFilterProps() throws Exception {
    Map<String, String> filterProps = new TreeMap<String, String>();
    filterProps.put(SOLR_PROXYUSER_PREFIX + "noGroups.hosts", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "anyHostAnyUser.groups", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "anyHostAnyUser.hosts", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "wrongHost.hosts", "1.1.1.1.1.1");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "wrongHost.groups", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "noHosts.groups", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "localHost.groups", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "localHost.hosts", "127.0.0.1");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "group.groups", getGroup());
    filterProps.put(SOLR_PROXYUSER_PREFIX + "group.hosts", "*");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "bogusGroup.groups", "__some_bogus_group");
    filterProps.put(SOLR_PROXYUSER_PREFIX + "bogusGroup.hosts", "*");
    return filterProps;
  }

  @BeforeClass
  public static void startup() throws Exception {
    Map<String, String> filterProps = getFilterProps();
    for (Map.Entry<String, String> entry : filterProps.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    String testHome = SolrTestCaseJ4.TEST_HOME();
    TreeMap<Class, String> extraRequestFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      public int compare(Class o1, Class o2) {
        return 0;
      }

      public boolean equals(Object obj) {
        return true;
      }
    });
    extraRequestFilters.put(HttpParamToRequestFilter.class, "*");
    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, new File(testHome, "solr-no-core.xml"),
      null, extraRequestFilters);
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
    Map<String, String> filterProps = getFilterProps();
    for (Map.Entry<String, String> entry : filterProps.entrySet()) {
      System.clearProperty(entry.getKey());
    }
  }

  private SolrRequest getProxyRequest(String user, String doAs, String remoteHost) {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpParamToRequestFilter.USER, user);
    params.set("doAs", doAs);
    if (remoteHost != null) params.set(HttpParamToRequestFilter.REMOTE_HOST, remoteHost);
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

  @Test
  public void testProxyNoConfigGroups() throws Exception {
    try {
      solrServer.request(getProxyRequest("noGroups","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("noGroups", "bar")));
    }
  }

  @Test
  public void testProxyWrongHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("wrongHost","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("wrongHost")));
    }
  }

  @Test
  public void testProxyNoConfigHosts() throws Exception {
    try {
      solrServer.request(getProxyRequest("noHosts","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      // FixMe: this should return an exception about the host being invalid,
      // but a bug (HADOOP-11077) causes an NPE instead.
      //assertTrue(ex.getMessage().contains(getExpectedHostExMsg("noHosts")));
    }
  }

  @Test
  public void testProxyValidateAnyHostAnyUser() throws Exception {
    solrServer.request(getProxyRequest("anyHostAnyUser", "bar", null));
  }

  @Test
  public void testProxyInvalidProxyUser() throws Exception {
    try {
      // wrong direction, should fail
      solrServer.request(getProxyRequest("bar","anyHostAnyUser", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("bar", "anyHostAnyUser")));
    }
  }

  @Test
  public void testProxyValidateHost() throws Exception {
    solrServer.request(getProxyRequest("localHost", "bar", null));
  }



  @Test
  public void testProxyValidateGroup() throws Exception {
    solrServer.request(getProxyRequest("group", System.getProperty("user.name"), null));
  }

  @Test
  public void testProxyUnknownHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("localHost", "bar", "unknownhost.bar.foo"));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("localHost")));
    }
  }

  @Test
  public void testProxyInvalidHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("localHost","bar", "[ff01::114]"));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("localHost")));
    }
  }

  @Test
  public void testProxyInvalidGroup() throws Exception {
    try {
      solrServer.request(getProxyRequest("bogusGroup","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("bogusGroup", "bar")));
    }
  }

  @Test
  public void testProxyNullProxyUser() throws Exception {
    try {
      solrServer.request(getProxyRequest("","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      // this exception is specific to our implementation, don't check a specific message.
    }
  }

  @Test
  public void testProxySuperUser() throws Exception {
    solrServer.request(getProxyRequest("solr", "bar", null));
  }

  private String getTokenQueryString(String baseURL, String user, String op,
      String delegation, String token, String renewer) {
    StringBuilder builder = new StringBuilder();
    builder.append(baseURL).append("/admin/cores?");
    if (user != null) {
      builder.append(HttpParamToRequestFilter.USER).append("=").append(user).append("&");
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

  // Ideally, we would do all of this via solrj, but solrj doesn't support
  // the delegation token commands yet
  private String getDelegationToken(String renewer, String user) throws Exception {
    HttpGet get = new HttpGet(getTokenQueryString(
      solrServer.getBaseURL(), user, "GETDELEGATIONTOKEN", null, null, renewer));
    HttpResponse response = getHttpResponse(get);
    assertNotNull(response);
    assertEquals(200, response.getStatusLine().getStatusCode());
    assertTrue(response.getEntity().getContentType().getValue().contains("application/json"));
    ObjectMapper mapper = new ObjectMapper();
    Map map = mapper.readValue(response.getEntity().getContent(), Map.class);
    String dt = (String) ((Map) map.get("Token")).get("urlString");
    EntityUtils.consumeQuietly(response.getEntity());
    return dt;
  }

  private void renewDelegationToken(String token, int expectedStatusCode, String user)
  throws Exception {
    HttpPut put = new HttpPut(getTokenQueryString(
      solrServer.getBaseURL(), user, "RENEWDELEGATIONTOKEN", null, token, null));
    HttpResponse response = getHttpResponse(put);
    assertNotNull(response);
    assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
    EntityUtils.consumeQuietly(response.getEntity());
  }

  private void cancelDelegationToken(String token, int expectedStatusCode)
  throws Exception {
    HttpPut put = new HttpPut(getTokenQueryString(
      solrServer.getBaseURL(), null, "CANCELDELEGATIONTOKEN", null, token, null));
    HttpResponse response = getHttpResponse(put);
    assertNotNull(response);
    assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
    EntityUtils.consumeQuietly(response.getEntity());
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

  /**
   * Test basic Delegation Token operations
   */
  @Test
  public void testDelegationTokens() throws Exception {
    final String user = "bar";

    // Get token
    String token = getDelegationToken(null, user);
    assertNotNull(token);

    // fail without token
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code);

    // pass with token
    doSolrRequest(token, 200);

    // pass with token on other server
    // FixMe: this should be 200 if we are using ZK to store the tokens,
    // see HADOOP-10868
    String otherServerUrl =
      miniCluster.getJettySolrRunners().get(1).getBaseUrl().toString();
    doSolrRequest(token, ErrorCode.FORBIDDEN.code, otherServerUrl);

    // renew token
    renewDelegationToken(token, 200, user);

    // pass with token
    doSolrRequest(token, 200);

    // pass with token on other server
    // FixMe: this should be 200 if we are using ZK to store the tokens,
    // see HADOOP-10868
    doSolrRequest(token, ErrorCode.FORBIDDEN.code, otherServerUrl);

    // cancel token, note don't need to be authenticated to cancel (no user specified)
    cancelDelegationToken(token, 200);

    // fail with token
    doSolrRequest(token, ErrorCode.FORBIDDEN.code);

    // fail without token
    doSolrRequest(null, ErrorCode.UNAUTHORIZED.code);
  }

  @Test
  public void testDelegationTokenCancelFail() throws Exception {
    // cancel twice
    String token = getDelegationToken(null, "bar");
    assertNotNull(token);
    cancelDelegationToken(token, 200);
    cancelDelegationToken(token, ErrorCode.NOT_FOUND.code);

    // cancel a non-existing token
    token = getDelegationToken(null, "bar");
    assertNotNull(token);

    cancelDelegationToken("BOGUS", ErrorCode.NOT_FOUND.code);
  }

  @Test
  public void testDelegationTokenRenew() throws Exception {
    // specify renewer and renew
    String user = "bar";
    String token = getDelegationToken(user, user);
    assertNotNull(token);
    renewDelegationToken(token, 200, user);
  }

  @Test
  public void testDelegationTokenRenewFail() throws Exception {
    // don't set renewer and try to renew as an a different user
    String token = getDelegationToken(null, "bar");
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "foo");

    // set renewer and try to renew as different user
    token = getDelegationToken("renewUser", "bar");
    assertNotNull(token);
    renewDelegationToken(token, ErrorCode.FORBIDDEN.code, "notRenewUser");
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
}
