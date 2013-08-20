package org.apache.solr.servlet;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyUserFilter implements Filter {

  private static final Logger LOG =
    LoggerFactory.getLogger(ProxyUserFilter.class);

  public static final String CONF_PREFIX = "solr.security.proxyuser.";
  public static final String GROUPS = ".groups";
  public static final String HOSTS = ".hosts";
  public static final String DO_AS_PARAM = "doAs";

  private Map<String, Set<String>> proxyUserHosts = new HashMap<String, Set<String>>();
  private Map<String, Set<String>> proxyUserGroups = new HashMap<String, Set<String>>();
  private org.apache.hadoop.security.Groups hGroups;


  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    
    for (Enumeration e = System.getProperties().propertyNames(); e.hasMoreElements();) {
      String key = e.nextElement().toString();
      if (key.startsWith(CONF_PREFIX) && key.endsWith(GROUPS)) {
        String proxyUser = key.substring(0, key.lastIndexOf(GROUPS));
        if (System.getProperty(proxyUser + HOSTS) == null) {
          throw new ServletException("Missing system property: " + proxyUser + HOSTS);
        }
        proxyUser = proxyUser.substring(CONF_PREFIX.length());
        String value = System.getProperty(key).trim();
        LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
        Set<String> values = null;
        if (!value.equals("*")) {
          values = new HashSet<String>(Arrays.asList(value.split(",")));
        }
        proxyUserGroups.put(proxyUser, values);
      }
      if (key.startsWith(CONF_PREFIX) && key.endsWith(HOSTS)) {
        String proxyUser = key.substring(0, key.lastIndexOf(HOSTS));
        if (System.getProperty(proxyUser + GROUPS) == null) {
          throw new ServletException("Missing system property: " + proxyUser + GROUPS);
        }
        proxyUser = proxyUser.substring(CONF_PREFIX.length());
        String value = System.getProperty(key).trim();
        LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
        Set<String> values = null;
        if (!value.equals("*")) {
          String[] hosts = value.split(",");
          for (int i = 0; i < hosts.length; i++) {
            String originalName = hosts[i];
            try {
              hosts[i] = normalizeHostname(originalName);
            }
            catch (Exception ex) {
              throw new ServletException("Could not normalize hostname", ex);
            }
            LOG.info("  Hostname, original [{}], normalized [{}]", originalName, hosts[i]);
          }
          values = new HashSet<String>(Arrays.asList(hosts));
        }
        proxyUserHosts.put(proxyUser, values);
      }
    }
    hGroups = new org.apache.hadoop.security.Groups(new Configuration());
  }

  /**
   * Verifies a proxyuser.
   *
   * @param proxyUser user name of the proxy user.
   * @param proxyHost host the proxy user is making the request from.
   * @param doAsUser user the proxy user is impersonating.
   * @throws IOException thrown if an error during the validation has occurred.
   * @throws AccessControlException thrown if the user is not allowed to perform the proxyuser request.
   */
  public void validate(String proxyUser, String proxyHost, String doAsUser)
  throws IOException, AccessControlException {
    checkNotEmpty(proxyUser, "proxyUser",
      "If you're attempting to use user-impersonation via a proxy user, please make sure that "
      + CONF_PREFIX + "#USER#.hosts and "
      + CONF_PREFIX + "#USER#.groups are configured correctly");
    checkNotEmpty(proxyHost, "proxyHost",
      "If you're attempting to use user-impersonation via a proxy user, please make sure that "
      + CONF_PREFIX + proxyUser + ".hosts and "
      + CONF_PREFIX + proxyUser + ".groups are configured correctly");
    checkNotEmpty(doAsUser, "doAsUser");
    LOG.debug("Authorization check proxyuser [{}] host [{}] doAs [{}]",
      new Object[]{proxyUser, proxyHost, doAsUser});
    if (proxyUserHosts.containsKey(proxyUser)) {
      proxyHost = normalizeHostname(proxyHost);
      validateRequestorHost(proxyUser, proxyHost, proxyUserHosts.get(proxyUser));
      validateGroup(proxyUser, doAsUser, proxyUserGroups.get(proxyUser));
    }
    else {
      throw new AccessControlException(MessageFormat.format("User [{0}] not defined as proxyuser", proxyUser));
    }
  }

  private void validateRequestorHost(String proxyUser, String hostname, Set<String> validHosts)
  throws IOException, AccessControlException {
    if (validHosts != null) {
      if (!validHosts.contains(hostname) && !validHosts.contains(normalizeHostname(hostname))) {
        throw new AccessControlException(MessageFormat.format(
          "Unauthorized host [{1}] for proxyuser [{2}]", hostname, proxyUser));
      }
    }
  }

  private void validateGroup(String proxyUser, String user, Set<String> validGroups)
  throws IOException, AccessControlException {
    if (validGroups != null) {
      List<String> userGroups = hGroups.getGroups(user);
      for (String g : validGroups) {
        if (userGroups.contains(g)) {
          return;
        }
      }
      throw new AccessControlException(MessageFormat.format(
        "Unauthorized proxyuser [{1}] for user [{2}], not in proxyuser groups", proxyUser, user));
    }
  }

  private String normalizeHostname(String name) {
    try {
      InetAddress address = InetAddress.getByName(name);
      return address.getCanonicalHostName();
    }
    catch (IOException ex) {
      throw new AccessControlException(MessageFormat.format(
        "Could not resolve host [{1}], {2}", name, ex.getMessage()));
    }
  }

  private String checkNotEmpty(String str, String name) {
    return checkNotEmpty(str, name, null);
  }

  /**
   * Check that a string is not null and not empty. If null or emtpy throws an IllegalArgumentException.
   *
   * @param str value.
   * @param name parameter name for the exception message.
   * @param info additional information to be printed with the exception message
   * @return the given value.
   */
  private String checkNotEmpty(String str, String name, String info) {
    if (str == null) {
      throw new IllegalArgumentException(name + " cannot be null" + (info == null ? "" : ", " + info));
    }
    if (str.length() == 0) {
      throw new IllegalArgumentException(name + " cannot be empty" + (info == null ? "" : ", " + info));
    }
    return str;
  }

  private String getRequestUrl(HttpServletRequest request) {
    StringBuffer url = request.getRequestURL();
    if (request.getQueryString() != null) {
      url.append("?").append(request.getQueryString());
    }
    return url.toString();
  }

  /**
   * Destroys the filter.
   */
  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
  throws IOException, ServletException {

    String userName = (String) request.getAttribute(SolrHadoopAuthenticationFilter.USER_NAME);
    HttpServletRequest httpRequest = (HttpServletRequest)request;
    final SolrParams params = SolrRequestParsers.parseQueryString(httpRequest.getQueryString());
    String doAsUserName = params.get(DO_AS_PARAM);
    if (doAsUserName != null && !doAsUserName.equals(userName)) {
      validate(userName, HostnameFilter.get(), doAsUserName);
      
      LOG.debug("Proxy user [{}] DoAs user [{}] Request [{}]",
        new Object[]{userName, doAsUserName, getRequestUrl((HttpServletRequest)request)});

      request.setAttribute(SolrHadoopAuthenticationFilter.USER_NAME, doAsUserName);
    }

    // Ensure the next filter in the chain (i.e. SolrRequestFilter) gets an
    // InputStream with all the content, as that is required.
    filterChain.doFilter(request, response);
  }
}
