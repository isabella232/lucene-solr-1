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
package org.apache.solr.client.solrj.impl;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.params.AuthPolicy;
import org.apache.solr.common.params.SolrParams;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kerberos-enabled HttpClientConfigurer
 */
public class Krb5HttpClientConfigurer extends HttpClientConfigurer {

  public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";
  private static final Logger logger = LoggerFactory
    .getLogger(HttpClientUtil.class);

  private static final ZKJaasConfiguration jaasConf = new ZKJaasConfiguration();

  protected void configure(DefaultHttpClient httpClient, SolrParams config) {
    super.configure(httpClient, config);

    if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
      final String basicAuthUser = config.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
      final String basicAuthPass = config.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
      // Setting both SPNego and basic auth not currently supported.  Prefer
      // SPNego; unset basic auth
      if (basicAuthUser != null && basicAuthPass != null) {
        logger.warn("Setting both SPNego auth and basic auth not supported.  "
          + "Preferring SPNego auth, ignoring basic auth.");
        httpClient.getCredentialsProvider().clear();
      }

      setSPNegoAuth(httpClient);
    }
  }

  /**
   * Set the http SPNego auth information.  If the java System prop
   * "java.security.auth.login.config" is null the auth information
   * is cleared.
   *
   * @return true if the SPNego credentials were set, false otherwise.
   */
  public static boolean setSPNegoAuth(DefaultHttpClient httpClient) {
    String configValue = System.getProperty(LOGIN_CONFIG_PROP);
    if (configValue != null) {
      logger.info("Setting up SPNego auth with config: " + configValue);
      final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
      String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

      // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
      // authentication mechanism can load the credentials from the JAAS configuration.
      if (useSubjectCredsVal == null) {
        System.setProperty(useSubjectCredsProp, "false");
      }
      else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
        // Don't overwrite the prop value if it's already been written to something else,
        // but log because it is likely the Credentials won't be loaded correctly.
        logger.warn("System Property: " + useSubjectCredsProp + " set to: " + useSubjectCredsVal
          + " not false.  SPNego authentication may not be successful.");
      }

      javax.security.auth.login.Configuration.setConfiguration(jaasConf);
      httpClient.getAuthSchemes().register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(true));
      // Get the credentials from the JAAS configuration rather than here
      Credentials use_jaas_creds = new Credentials() {
        public String getPassword() {
          return null;
        }

        public Principal getUserPrincipal() {
          return null;
        }
      };

      httpClient.getCredentialsProvider().setCredentials(
        AuthScope.ANY, use_jaas_creds);
      return true;
    } else {
      httpClient.getCredentialsProvider().clear();
    }
    return false;
  }

  /**
   * Use the ZK JAAS Client appName rather than the default com.sun.security.jgss appName.
   * This simplifies client configuration; rather than requiring an appName in the JAAS conf
   * for ZooKeeper's client (default "Client") and one for the default com.sun.security.jgss
   * appName, only a single unified appName is needed.
   */
  private static class ZKJaasConfiguration extends javax.security.auth.login.Configuration {

    private javax.security.auth.login.Configuration baseConfig;
    private String zkClientLoginContext;

    // the com.sun.security.jgss appNames
    private Set<String> initiateAppNames = new HashSet<String>(
      Arrays.asList("com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate"));

    public ZKJaasConfiguration() {
      try {
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }

      // Get the app name for the ZooKeeperClient
      zkClientLoginContext = System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");
      logger.debug("ZK client login context is: " + zkClientLoginContext);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (baseConfig == null) return null;

      if (initiateAppNames.contains(appName)) {
        logger.debug("Using AppConfigurationEntry for appName: " + zkClientLoginContext
          + " instead of: " + appName);
        return baseConfig.getAppConfigurationEntry(zkClientLoginContext);
      }
      return baseConfig.getAppConfigurationEntry(appName);
    }
  }
}
