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

package org.apache.solr.cloud;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
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

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;

import org.eclipse.jetty.servlet.ServletHolder;

/**
 * MiniSolrCloudCluster that supports HttpParam (via USER_PARAM http param)
 * and DelegationToken authentication.
 */
public class HttpParamDelegationTokenMiniSolrCloudCluster extends MiniSolrCloudCluster {
  public static final String USER_PARAM = "user";
  public static final String REMOTE_HOST_PARAM = "remoteHost";

  public HttpParamDelegationTokenMiniSolrCloudCluster(int numServers, String hostContext,
      File solrXml, SortedMap<ServletHolder, String> extraServlets) throws Exception {
    super(numServers, hostContext, solrXml, extraServlets, null);
  }

  @Override
  public JettySolrRunner startJettySolrRunner(String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    if (extraRequestFilters != null) {
      throw new IllegalArgumentException("non-null extra request filters not supported");
    }
    TreeMap<Class, String> extraFilters = new TreeMap<Class, String>(new Comparator<Class>() {
      // There's only one class, make this as simple as possible
      public int compare(Class o1, Class o2) {
        return 0;
      }

      public boolean equals(Object obj) {
        return true;
      }
    });
    extraFilters.put(HttpParamToRequestFilter.class, "*");
    return super.startJettySolrRunner(hostContext, extraServlets, extraFilters);
  }

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
      String userName = getHttpParam(request, USER_PARAM);
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

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = super.getConfiguration(configPrefix, filterConfig);
      conf.setProperty(AUTH_TYPE,
          HttpParamDelegationTokenAuthenticationHandler.class.getName());
      return conf;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
    throws IOException, ServletException {
      final HttpServletRequest httpRequest = (HttpServletRequest)request;
      final HttpServletRequestWrapper requestWrapper = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getRemoteHost() {
          String param = getHttpParam(httpRequest, REMOTE_HOST_PARAM);
          return param != null ? param : httpRequest.getRemoteHost();
        }
      };

      super.doFilter(requestWrapper, (HttpServletResponse)response, chain);
    }
  }
}
