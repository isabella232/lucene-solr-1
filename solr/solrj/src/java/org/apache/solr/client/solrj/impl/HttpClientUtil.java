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

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.client.params.ClientParamBean;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager; // jdoc
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating/configuring httpclient instances. 
 */
public class HttpClientUtil {
  
  // socket timeout measured in ms, closes a socket if read
  // takes longer than x ms to complete. throws
  // java.net.SocketTimeoutException: Read timed out exception
  public static final String PROP_SO_TIMEOUT = "socketTimeout";
  // connection timeout measures in ms, closes a socket if connection
  // cannot be established within x ms. with a
  // java.net.SocketTimeoutException: Connection timed out
  public static final String PROP_CONNECTION_TIMEOUT = "connTimeout";
  // Maximum connections allowed per host
  public static final String PROP_MAX_CONNECTIONS_PER_HOST = "maxConnectionsPerHost";
  // Maximum total connections allowed
  public static final String PROP_MAX_CONNECTIONS = "maxConnections";
  // Retry http requests on error
  public static final String PROP_USE_RETRY = "retry";
  // Allow compression (deflate,gzip) if server supports it
  public static final String PROP_ALLOW_COMPRESSION = "allowCompression";
  // Follow redirects
  public static final String PROP_FOLLOW_REDIRECTS = "followRedirects";
  // Basic auth username 
  public static final String PROP_BASIC_AUTH_USER = "httpBasicAuthUser";
  // Basic auth password 
  public static final String PROP_BASIC_AUTH_PASS = "httpBasicAuthPassword";
  
  public static final String SYS_PROP_CHECK_PEER_NAME = "solr.ssl.checkPeerName";
  
  private static final Logger logger = LoggerFactory
      .getLogger(HttpClientUtil.class);
  
  static final DefaultHttpRequestRetryHandler NO_RETRY = new DefaultHttpRequestRetryHandler(
      0, false);

  private static HttpClientConfigurer configurer = new HttpClientConfigurer();
  
  private HttpClientUtil(){}
  
  /**
   * Replace the {@link HttpClientConfigurer} class used in configuring the http
   * clients with a custom implementation.
   */
  public static void setConfigurer(HttpClientConfigurer newConfigurer) {
    configurer = newConfigurer;
  }
  
  public static HttpClientConfigurer getConfigurer() {
    return configurer;
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   * @param params
   *          http client configuration, if null a client with default
   *          configuration (no additional configuration) is created. 
   */
  public static HttpClient createClient(final SolrParams params) {
    final ModifiableSolrParams config = new ModifiableSolrParams(params);
    if (logger.isDebugEnabled()) {
      logger.debug("Creating new http client, config:" + config);
    }
    final DefaultHttpClient httpClient = new SystemDefaultHttpClient();
    configureClient(httpClient, config);
    return httpClient;
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   */
  public static HttpClient createClient(final SolrParams params, ClientConnectionManager cm) {
    final ModifiableSolrParams config = new ModifiableSolrParams(params);
    if (logger.isDebugEnabled()) {
      logger.debug("Creating new http client, config:" + config);
    }
    final DefaultHttpClient httpClient = new DefaultHttpClient(cm);
    configureClient(httpClient, config);
    return httpClient;
  }

  /**
   * Configures {@link DefaultHttpClient}, only sets parameters if they are
   * present in config.
   */
  public static void configureClient(final DefaultHttpClient httpClient,
      SolrParams config) {
    configurer.configure(httpClient,  config);
  }

  /**
   * Control HTTP payload compression.
   * 
   * @param allowCompression
   *          true will enable compression (needs support from server), false
   *          will disable compression.
   */
  public static void setAllowCompression(DefaultHttpClient httpClient,
      boolean allowCompression) {
    httpClient
        .removeRequestInterceptorByClass(UseCompressionRequestInterceptor.class);
    httpClient
        .removeResponseInterceptorByClass(UseCompressionResponseInterceptor.class);
    if (allowCompression) {
      httpClient.addRequestInterceptor(new UseCompressionRequestInterceptor());
      httpClient
          .addResponseInterceptor(new UseCompressionResponseInterceptor());
    }
  }

  /**
   * Set http basic auth information. If basicAuthUser or basicAuthPass is null
   * the basic auth configuration is cleared. Currently this is not preemtive
   * authentication. So it is not currently possible to do a post request while
   * using this setting.
   */
  public static void setBasicAuth(DefaultHttpClient httpClient,
      String basicAuthUser, String basicAuthPass) {
    if (basicAuthUser != null && basicAuthPass != null) {
      httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(basicAuthUser, basicAuthPass));
    } else {
      httpClient.getCredentialsProvider().clear();
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
    final String configProp = "java.security.auth.login.config";
    String configValue = System.getProperty(configProp);
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

      ZKJaasConfiguration jaasConf = new ZKJaasConfiguration();
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
   * Set max connections allowed per host. This call will only work when
   * {@link ThreadSafeClientConnManager} or
   * {@link PoolingClientConnectionManager} is used.
   */
  public static void setMaxConnectionsPerHost(HttpClient httpClient,
      int max) {
    // would have been nice if there was a common interface
    if (httpClient.getConnectionManager() instanceof ThreadSafeClientConnManager) {
      ThreadSafeClientConnManager mgr = (ThreadSafeClientConnManager)httpClient.getConnectionManager();
      mgr.setDefaultMaxPerRoute(max);
    } else if (httpClient.getConnectionManager() instanceof PoolingClientConnectionManager) {
      PoolingClientConnectionManager mgr = (PoolingClientConnectionManager)httpClient.getConnectionManager();
      mgr.setDefaultMaxPerRoute(max);
    }
  }

  /**
   * Set max total connections allowed. This call will only work when
   * {@link ThreadSafeClientConnManager} or
   * {@link PoolingClientConnectionManager} is used.
   */
  public static void setMaxConnections(final HttpClient httpClient,
      int max) {
    // would have been nice if there was a common interface
    if (httpClient.getConnectionManager() instanceof ThreadSafeClientConnManager) {
      ThreadSafeClientConnManager mgr = (ThreadSafeClientConnManager)httpClient.getConnectionManager();
      mgr.setMaxTotal(max);
    } else if (httpClient.getConnectionManager() instanceof PoolingClientConnectionManager) {
      PoolingClientConnectionManager mgr = (PoolingClientConnectionManager)httpClient.getConnectionManager();
      mgr.setMaxTotal(max);
    }
  }
  

  /**
   * Defines the socket timeout (SO_TIMEOUT) in milliseconds. A timeout value of
   * zero is interpreted as an infinite timeout.
   * 
   * @param timeout timeout in milliseconds
   */
  public static void setSoTimeout(HttpClient httpClient, int timeout) {
    HttpConnectionParams.setSoTimeout(httpClient.getParams(),
        timeout);
  }

  /**
   * Control retry handler 
   * @param useRetry when false the client will not try to retry failed requests.
   */
  public static void setUseRetry(final DefaultHttpClient httpClient,
      boolean useRetry) {
    if (!useRetry) {
      httpClient.setHttpRequestRetryHandler(NO_RETRY);
    } else {
      httpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler());
    }
  }

  /**
   * Set connection timeout. A timeout value of zero is interpreted as an
   * infinite timeout.
   * 
   * @param timeout
   *          connection Timeout in milliseconds
   */
  public static void setConnectionTimeout(final HttpClient httpClient,
      int timeout) {
      HttpConnectionParams.setConnectionTimeout(httpClient.getParams(),
          timeout);
  }

  /**
   * Set follow redirects.
   *
   * @param followRedirects  When true the client will follow redirects.
   */
  public static void setFollowRedirects(HttpClient httpClient,
      boolean followRedirects) {
    new ClientParamBean(httpClient.getParams()).setHandleRedirects(followRedirects);
  }

  public static void setHostNameVerifier(DefaultHttpClient httpClient,
      X509HostnameVerifier hostNameVerifier) {
    Scheme httpsScheme = httpClient.getConnectionManager().getSchemeRegistry().get("https");
    if (httpsScheme != null) {
      SSLSocketFactory sslSocketFactory = (SSLSocketFactory) httpsScheme.getSchemeSocketFactory();
      sslSocketFactory.setHostnameVerifier(hostNameVerifier);
    }
  }
  
  private static class UseCompressionRequestInterceptor implements
      HttpRequestInterceptor {
    
    @Override
    public void process(HttpRequest request, HttpContext context)
        throws HttpException, IOException {
      if (!request.containsHeader("Accept-Encoding")) {
        request.addHeader("Accept-Encoding", "gzip, deflate");
      }
    }
  }
  
  private static class UseCompressionResponseInterceptor implements
      HttpResponseInterceptor {
    
    @Override
    public void process(final HttpResponse response, final HttpContext context)
        throws HttpException, IOException {
      
      HttpEntity entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      if (ceheader != null) {
        HeaderElement[] codecs = ceheader.getElements();
        for (int i = 0; i < codecs.length; i++) {
          if (codecs[i].getName().equalsIgnoreCase("gzip")) {
            response
                .setEntity(new GzipDecompressingEntity(response.getEntity()));
            return;
          }
          if (codecs[i].getName().equalsIgnoreCase("deflate")) {
            response.setEntity(new DeflateDecompressingEntity(response
                .getEntity()));
            return;
          }
        }
      }
    }
  }
  
  private static class GzipDecompressingEntity extends HttpEntityWrapper {
    public GzipDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }
    
    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      return new GZIPInputStream(wrappedEntity.getContent());
    }
    
    @Override
    public long getContentLength() {
      return -1;
    }
  }
  
  private static class DeflateDecompressingEntity extends
      GzipDecompressingEntity {
    public DeflateDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }
    
    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      return new InflaterInputStream(wrappedEntity.getContent());
    }
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
