
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import static org.apache.solr.servlet.SolrHadoopAuthenticationFilter.SOLR_PROXYUSER_PREFIX;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
public class SolrHadoopAuthenticationFilterProxyTest extends SolrTestCaseJ4 {
  private static Logger log = LoggerFactory.getLogger(SolrHadoopAuthenticationFilterProxyTest.class);
  private static final int NUM_SERVERS = 1;
  private static MiniSolrCloudCluster miniCluster;
  private static HttpSolrServer solrServer;

  /**
   * Filter that converts http params to HttpServletRequest params
   */
  public static class ParamsToRequestFilter extends SolrHadoopAuthenticationFilter {
    public static final String USER = "user";
    public static final String REMOTE_HOST = "remoteHost";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      super.init(filterConfig);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
      final HttpServletRequest httpRequest = (HttpServletRequest)request;
      final HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public Principal getUserPrincipal() {
          String userName = getHttpParam(USER);
          return new AuthenticationToken(userName, userName, "simple");
        }

        @Override
        public String getRemoteHost() {
          String param = getHttpParam(REMOTE_HOST);
          return param != null ? param : httpRequest.getRemoteHost();
        }

        private String getHttpParam(String param) {
          List<NameValuePair> pairs =
            URLEncodedUtils.parse(httpRequest.getQueryString(), Charset.forName("UTF-8"));
          for (NameValuePair nvp : pairs) {
            if(param.equals(nvp.getName())) {
              return nvp.getValue();
            }
          }
          return null;
        }
      };

      super.doFilter(chain, wrapper, (HttpServletResponse)response);
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
    extraRequestFilters.put(ParamsToRequestFilter.class, "*");
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
    params.set(ParamsToRequestFilter.USER, user);
    params.set("doAs", doAs);
    if (remoteHost != null) params.set(ParamsToRequestFilter.REMOTE_HOST, remoteHost);
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
  public void testNoConfigGroups() throws Exception {
    try {
      solrServer.request(getProxyRequest("noGroups","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("noGroups", "bar")));
    }
  }

  @Test
  public void testWrongHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("wrongHost","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("wrongHost")));
    }
  }

  @Test
  public void testNoConfigHosts() throws Exception {
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
  public void testValidateAnyHostAnyUser() throws Exception {
    solrServer.request(getProxyRequest("anyHostAnyUser", "bar", null));
  }

  @Test
  public void testInvalidProxyUser() throws Exception {
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
  public void testValidateHost() throws Exception {
    solrServer.request(getProxyRequest("localHost", "bar", null));
  }



  @Test
  public void testValidateGroup() throws Exception {
    solrServer.request(getProxyRequest("group", System.getProperty("user.name"), null));
  }

  @Test
  public void testUnknownHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("localHost", "bar", "unknownhost.bar.foo"));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("localHost")));
    }
  }

  @Test
  public void testInvalidHost() throws Exception {
    try {
      solrServer.request(getProxyRequest("localHost","bar", "[ff01::114]"));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedHostExMsg("localHost")));
    }
  }

  @Test
  public void testInvalidGroup() throws Exception {
    try {
      solrServer.request(getProxyRequest("bogusGroup","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains(getExpectedGroupExMsg("bogusGroup", "bar")));
    }
  }

  @Test
  public void testNullProxyUser() throws Exception {
    try {
      solrServer.request(getProxyRequest("","bar", null));
      fail("Expected RemoteSolrException");
    }
    catch (HttpSolrServer.RemoteSolrException ex) {
      // this exception is specific to our implementation, don't check a specific message.
    }
  }

  @Test
  public void testSuperUser() throws Exception {
    solrServer.request(getProxyRequest("solr", "bar", null));
  }
}
