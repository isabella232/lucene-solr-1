package org.apache.solr.servlet;
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import static org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter.PROXYUSER_PREFIX;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.solr.servlet.SolrRequestParsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication filter that extends Hadoop-auth AuthenticationFilter to override
 * the configuration loading.
 */
public class SolrHadoopAuthenticationFilter extends DelegationTokenAuthenticationFilter {
  private static Logger LOG = LoggerFactory.getLogger(SolrHadoopAuthenticationFilter.class);
  public static final String SOLR_PREFIX = "solr.authentication.";
  public static final String SOLR_PROXYUSER_PREFIX = "solr.security.proxyuser.";

  private boolean skipAuthFilter = false;
  
  // The ProxyUserFilter can't handle options, let's handle it here
  private HttpServlet optionsServlet;

  private static String superUser = System.getProperty("solr.authorization.superuser", "solr");

  /**
   * Request attribute constant for the user name.
   */
  public static final String USER_NAME = "solr.user.name";

  /**
   * Http param for requesting ProxyUser support.
   */
  public static final String DO_AS_PARAM = "doAs";

  /**
   * Initialize the filter.
   *
   * @param filterConfig filter configuration.
   * @throws ServletException thrown if the filter could not be initialized.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    super.init(filterConfig);
    optionsServlet = new HttpServlet() {};
    optionsServlet.init();
    // ensure the admin requests add the request
    SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(true);
  }

  /**
   * Destroy the filter.
   */
  @Override
  public void destroy() {
    optionsServlet.destroy();
    super.destroy();
  }

  /**
   * Return the ProxyUser Configuration.  System properties beginning with
   * {@link SOLR_PROXYUSER_PREFIX} will be added to the configuration.
   */
  @Override
  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig)
      throws ServletException {
    Configuration conf = new Configuration(false);
    for (Enumeration e = System.getProperties().propertyNames(); e.hasMoreElements();) {
      String key = e.nextElement().toString();
      if (key.startsWith(SOLR_PROXYUSER_PREFIX)) {
        conf.set(PROXYUSER_PREFIX + "." + key.substring(SOLR_PROXYUSER_PREFIX.length()),
          System.getProperty(key));
      }
    }
    // superuser must be able to proxy any user in order to properly
    // forward requests
    final String superUserGroups = PROXYUSER_PREFIX + "." + superUser + ".groups";
    final String superUserHosts = PROXYUSER_PREFIX + "." + superUser + ".hosts";
    if (conf.get(superUserGroups) == null && conf.get(superUserHosts) == null) {
      conf.set(superUserGroups, "*");
      conf.set(superUserHosts, "*");
    } else {
      LOG.warn("Not automatically granting proxy privileges to superUser: " + superUser
        + " because user groups or user hosts already set for superUser");
    }
    return conf;
  }

  /**
   * Returns the System properties to be used by the authentication filter.
   * <p/>
   * All properties from the System properties with names that starts with {@link #SOLR_PREFIX} will
   * be returned. The keys of the returned properties are trimmed from the {@link #SOLR_PREFIX}
   * prefix, for example the property name 'solr.authentication.type' will
   * be just 'type'.
   *
   * @param configPrefix configuration prefix, this parameter is ignored by this implementation.
   * @param filterConfig filter configuration, this parameter is ignored by this implementation.
   * @return all System properties prefixed with {@link #SOLR_PREFIX}, without the
   * prefix.
   */
  @Override
  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) {
    Properties props = new Properties();

    //setting the cookie path to root '/' so it is used for all resources.
    props.setProperty(AuthenticationFilter.COOKIE_PATH, "/");

    for (String name : System.getProperties().stringPropertyNames()) {
      if (name.startsWith(SOLR_PREFIX)) {
        String value = System.getProperty(name);
        name = name.substring(SOLR_PREFIX.length());
        props.setProperty(name, value);
      }
    }

    // ensure old behavior (simple authentication) if properties not specified
    if (props.getProperty(AUTH_TYPE) == null) {
      props.setProperty(AUTH_TYPE, PseudoAuthenticationHandler.TYPE);
      if (props.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED) == null) {
        props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
      }
    }

    // use QueryStringAuthenticationHandler rather than hadoop's
    // PseudoAuthenticationHandler, so we don't affect getInputStream()
    if (props.getProperty(AUTH_TYPE).equals(PseudoAuthenticationHandler.TYPE)) {
      props.setProperty(AUTH_TYPE, QueryStringAuthenticationHandler.TYPE);
    }

    return props;
  }

  /**
   * Enforces authentication using Hadoop-auth AuthenticationFilter.
   *
   * @param request http request.
   * @param response http response.
   * @param filterChain filter chain.
   * @throws IOException thrown if an IO error occurs.
   * @throws ServletException thrown if a servlet error occurs.
   */
  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
      throws IOException, ServletException {

    FilterChain filterChainWrapper = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
          throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        if (httpRequest.getMethod().equals("OPTIONS")) {
          optionsServlet.service(request, response);
        }
        else {
          httpRequest.setAttribute(USER_NAME, httpRequest.getRemoteUser());
          filterChain.doFilter(servletRequest, servletResponse);
        }
      }
    };

    super.doFilter(request, response, filterChainWrapper);
  }
}
