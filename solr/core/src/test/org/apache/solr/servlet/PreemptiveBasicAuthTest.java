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

package org.apache.solr.servlet;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.TreeMap;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.HttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.apache.solr.client.solrj.impl.PreemptiveBasicAuthConfigurer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This unit test verifies the functionality of {@link PreemptiveBasicAuthConfigurer}.
 * Specifically it starts a Solr server instance configured with a custom {@linkplain Filter}
 * to verify the presence of Http 'Authorization' header with appropriate credentials. As part
 * of the test, a solrj client sends a sample STATUS command. This test passes only if
 * {@link PreemptiveBasicAuthConfigurer} is configured before creating a Solr client.
 */
@SolrTestCaseJ4.SuppressSSL
public class PreemptiveBasicAuthTest extends SolrTestCaseJ4 {
  private static MiniSolrCloudCluster miniCluster;
  private static JettySolrRunner runner;
  static final String TEST_USER_NAME = "test1";
  static final String TEST_USER_PASSWD = "passwd";

  @BeforeClass
  public static void beforeTest() throws Exception {
    TreeMap<Class, String> extraFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      public int compare(Class o1, Class o2) {
        return 0;
      }

      public boolean equals(Object obj) {
        return true;
      }
    });
    extraFilters.put(VerifyBasicAuthCredentials.class, "*");

    String testHome = SolrTestCaseJ4.TEST_HOME();
    miniCluster = new MiniSolrCloudCluster(1, null, createTempDir().toPath(), new File(testHome, "solr-no-core.xml"),
      null, extraFilters);
    runner = miniCluster.getJettySolrRunners().get(0);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    runner = null;
  }

  @Test
  public void testPreemptiveBasicAuth() throws Exception {
    HttpClientConfigurer prev = HttpClientUtil.getConfigurer();
    HttpSolrServer server = null;

    try {
      HttpClientUtil.setConfigurer(new PreemptiveBasicAuthConfigurer());
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, TEST_USER_NAME);
      params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, TEST_USER_PASSWD);
      PreemptiveBasicAuthConfigurer.setDefaultSolrParams(params);

      server = new HttpSolrServer(runner.getBaseUrl().toString());
      server.request(getStatusQuery()); // The request must succeed.
    } finally {
      if (server != null) {
        server.shutdown();
      }
      HttpClientUtil.setConfigurer(prev);
      PreemptiveBasicAuthConfigurer.setDefaultSolrParams(new ModifiableSolrParams());
    }
  }

  @Test
  public void testBasicAuth() throws Exception {
    HttpClientConfigurer prev = HttpClientUtil.getConfigurer();
    HttpSolrServer server = null;
    HttpClient extClient = null;

    try {
      //Don't use preemptive auth support.
      HttpClientUtil.setConfigurer(new HttpClientConfigurer());
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, TEST_USER_NAME);
      params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, TEST_USER_PASSWD);
      //Due to SOLR-8056, we need to use external client.
      extClient = HttpClientUtil.createClient(params);

      server = new HttpSolrServer(runner.getBaseUrl().toString(), extClient);
      server.request(getStatusQuery()); // The request must fail.
      fail("This request must fail due to authentication error.");
    } catch (RemoteSolrException ex) {
      Assert.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, ex.code());
    }finally {
      if (server != null) {
        server.shutdown();
      }
      if(extClient != null) {
        extClient.getConnectionManager().shutdown();
      }
      HttpClientUtil.setConfigurer(prev);
    }
  }

  private QueryRequest getStatusQuery() {
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.STATUS.toString());
    QueryRequest request = new QueryRequest(queryParams);
    request.setPath("/admin/cores");
    return request;
  }

  /**
   * A Http request intercepter implementation which verifies that every outgoing
   * HTTP request is associated with basic auth credentials.
   */
  public static class VerifyBasicAuthCredentials implements Filter {
    /**
     * Constant that identifies the HTTP authentication scheme.
     */
    public static final String BASIC_AUTH_SCHEME = "Basic";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String WWW_AUTHENTICATION_HEADER = "WWW-Authenticate";

    @Override
    public void destroy() {}

    @Override
    public void init(FilterConfig arg0) throws ServletException {}

    @Override
    public void doFilter(ServletRequest arg0, ServletResponse arg1,
        FilterChain arg2) throws IOException, ServletException {
      HttpServletRequest httpReq = (HttpServletRequest)arg0;
      HttpServletResponse httpResp = (HttpServletResponse)arg1;

      String authorization = httpReq.getHeader(AUTHORIZATION_HEADER);
      if(authorization == null || !authorization.startsWith(BASIC_AUTH_SCHEME)) {
        httpResp.addHeader(WWW_AUTHENTICATION_HEADER, BASIC_AUTH_SCHEME);
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      } else {
        authorization = authorization.substring(BASIC_AUTH_SCHEME.length()).trim();
        final Base64 base64 = new Base64(0);
        String[] credentials =
            new String(base64.decode(authorization), StandardCharsets.UTF_8).split(":", 2);
        if(credentials.length == 2 && validate(credentials[0], credentials[1])) {
          arg2.doFilter(arg0, arg1);
        } else {
          httpResp.addHeader(WWW_AUTHENTICATION_HEADER, BASIC_AUTH_SCHEME);
          httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        }
      }
    }

    private boolean validate(String userName, String password) {
      return TEST_USER_NAME.equals(userName) && TEST_USER_PASSWD.equals(password);
    }
  }
}
