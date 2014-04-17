package org.apache.solr.util;

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

import java.io.File;
import java.util.Random;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SecureRandomSpi;
import java.security.UnrecoverableKeyException;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.solr.shaded.org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.solr.shaded.org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.solr.shaded.org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientUtil;

import org.eclipse.jetty.util.resource.Resource;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.params.SolrParams;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class SSLTestConfig extends SSLConfig {
  public static File TEST_KEYSTORE = ExternalPaths.EXAMPLE_HOME == null ? null
      : new File(ExternalPaths.EXAMPLE_HOME, "../etc/solrtest.keystore");
  
  private static String TEST_KEYSTORE_PATH = TEST_KEYSTORE != null
      && TEST_KEYSTORE.exists() ? TEST_KEYSTORE.getAbsolutePath() : null;
  private static String TEST_KEYSTORE_PASSWORD = "secret";
  private static HttpClientConfigurer DEFAULT_CONFIGURER = new HttpClientConfigurer();
  
  public SSLTestConfig() {
    this(false, false);
  }
  
  public SSLTestConfig(boolean useSSL, boolean clientAuth) {
    super(useSSL, clientAuth, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD);
  }
 
  public SSLTestConfig(boolean useSSL, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    super(useSSL, clientAuth, keyStore, keyStorePassword, trustStore, trustStorePassword);
  }
  
  /**
   * Will provide an HttpClientConfigurer for SSL support (adds https and
   * removes http schemes) is SSL is enabled, otherwise return the default
   * configurer
   */
  public HttpClientConfigurer getHttpClientConfigurer() {
    return isSSLMode() ? new SSLHttpClientConfigurer() : DEFAULT_CONFIGURER;
  }

  /**
   * Builds a new SSLContext for HTTP <b>clients</b> to use when communicating with servers which have 
   * been configured based on the settings of this object.  
   *
   * NOTE: Uses a completely insecure {@link SecureRandom} instance to prevent tests from blocking 
   * due to lack of entropy, also explicitly allows the use of self-signed 
   * certificates (since that's what is almost always used during testing).
   */
  protected SSLContext buildSSLContext() throws KeyManagementException, 
    UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {
    
    SSLContextBuilder builder = SSLContexts.custom();
    builder.setSecureRandom(NotSecurePsuedoRandom.INSTANCE);
    
    // NOTE: KeyStore & TrustStore are swapped because they are from configured from server perspective...
    // we are a client - our keystore contains the keys the server trusts, and vice versa
    builder.loadTrustMaterial(buildKeyStore(getKeyStore(), getKeyStorePassword()), new TrustSelfSignedStrategy()).build();

    if (isClientAuthMode()) {
      builder.loadKeyMaterial(buildKeyStore(getTrustStore(), getTrustStorePassword()), getTrustStorePassword().toCharArray());
      
    }

    return builder.build();
  }
  
  /**
   * Builds a new SSLContext for jetty servers which have been configured based on the settings of 
   * this object.
   *
   * NOTE: Uses a completely insecure {@link SecureRandom} instance to prevent tests from blocking 
   * due to lack of entropy, also explicitly allows the use of self-signed 
   * certificates (since that's what is almost always used during testing).
   * almost always used during testing). 
   */
  public SSLContext buildServerSSLContext() throws KeyManagementException, 
    UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {

    assert isSSLMode();
    
    SSLContextBuilder builder = SSLContexts.custom();
    builder.setSecureRandom(NotSecurePsuedoRandom.INSTANCE);
    
    builder.loadKeyMaterial(buildKeyStore(getKeyStore(), getKeyStorePassword()), getKeyStorePassword().toCharArray());

    if (isClientAuthMode()) {
      builder.loadTrustMaterial(buildKeyStore(getTrustStore(), getTrustStorePassword()), new TrustSelfSignedStrategy()).build();
      
    }

    return builder.build();
  }

  /**
   * Returns an SslContextFactory using {@link #buildServerSSLContext} if SSL should be used, else returns null.
   */
  @Override
  public SslContextFactory createContextFactory() {
    if (!isSSLMode()) {
      return null;
    }
    // else...

    
    SslContextFactory factory = new SslContextFactory(false);
    try {
      factory.setSslContext(buildServerSSLContext());
    } catch (Exception e) { 
      throw new RuntimeException("ssl context init failure: " + e.getMessage(), e); 
    }
    factory.setNeedClientAuth(isClientAuthMode());
    return factory;
  }
  
  /**
   * Constructs a KeyStore using the specified filename and password
   */
  protected static KeyStore buildKeyStore(String keyStoreLocation, String password) {
    try {
      return CertificateUtils.getKeyStore(null, keyStoreLocation, "JKS", null, password);
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to build KeyStore from file: " + keyStoreLocation, ex);
    }
  }
  
  private class SSLHttpClientConfigurer extends HttpClientConfigurer {
    @SuppressWarnings("deprecation")
    protected void configure(DefaultHttpClient httpClient, SolrParams config) {
      super.configure(httpClient, config);
      SchemeRegistry registry = httpClient.getConnectionManager().getSchemeRegistry();
      // Make sure no tests cheat by using HTTP
      registry.unregister("http");
      try {
        registry.register(new Scheme("https", 443, new SSLSocketFactory(buildSSLContext())));
      } catch (KeyManagementException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (UnrecoverableKeyException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (NoSuchAlgorithmException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (KeyStoreException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      }
    }
  }
  
  public static void setSSLSystemProperties() {
    System.setProperty("javax.net.ssl.keyStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.keyStorePassword", TEST_KEYSTORE_PASSWORD);
    System.setProperty("javax.net.ssl.trustStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.trustStorePassword", TEST_KEYSTORE_PASSWORD);
  }
  
  public static void clearSSLSystemProperties() {
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
  }

  /**
   * A mocked up instance of SecureRandom that just uses {@link Random} under the covers.
   * This is to prevent blocking issues that arise in platform default 
   * SecureRandom instances due to too many instances / not enough random entropy.  
   * Tests do not need secure SSL.
   */
  private static class NotSecurePsuedoRandom extends SecureRandom {
    public static final SecureRandom INSTANCE = new NotSecurePsuedoRandom();
    private static final Random RAND = new Random(42);
    
    /** 
     * Helper method that can be used to fill an array with non-zero data.
     * (Attempted workarround of Solaris SSL Padding bug: SOLR-9068)
     */
    private static final byte[] fillData(byte[] data) {
      RAND.nextBytes(data);
      return data;
    }
    
    /** SPI Used to init all instances */
    private static final SecureRandomSpi NOT_SECURE_SPI = new SecureRandomSpi() {
      /** returns a new byte[] filled with static data */
      public byte[] engineGenerateSeed(int numBytes) {
        return fillData(new byte[numBytes]);
      }
      /** fills the byte[] with static data */
      public void engineNextBytes(byte[] bytes) {
        fillData(bytes);
      }
      /** NOOP */
      public void engineSetSeed(byte[] seed) { /* NOOP */ }
    };
    
    private NotSecurePsuedoRandom() {
      super(NOT_SECURE_SPI, null) ;
    }
    
    /** returns a new byte[] filled with static data */
    public byte[] generateSeed(int numBytes) {
      return fillData(new byte[numBytes]);
    }
    /** fills the byte[] with static data */
    synchronized public void nextBytes(byte[] bytes) {
      fillData(bytes);
    }
    /** NOOP */
    synchronized public void setSeed(byte[] seed) { /* NOOP */ }
    /** NOOP */
    synchronized public void setSeed(long seed) { /* NOOP */ }
    
  }
}
