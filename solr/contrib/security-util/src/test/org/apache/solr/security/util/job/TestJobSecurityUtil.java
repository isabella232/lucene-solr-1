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
package org.apache.solr.security.util.job;

import org.apache.commons.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;


public class TestJobSecurityUtil extends SolrCloudTestCase {
  private static final int NUM_SERVERS = 2;
  private static HttpSolrClient userAuthServer;
  private static String baseURL;
  private static String zkHost;

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeFalse("Hadoop does not work on Windows", Constants.WINDOWS);
    assumeFalse("FIXME: SOLR-8182: This test fails under Java 9", Constants.JRE_IS_MINIMUM_JAVA9);

    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(getFile("solr/hadoop_simple_auth_config.json").toPath())
        .configure();

    baseURL = cluster.getJettySolrRunner(0).getBaseUrl().toString();
    zkHost = cluster.getZkServer().getZkAddress();

    userAuthServer = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL)
                                                   .withInvariantParams(getAuthParams())
                                                   .build();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (userAuthServer != null) {
      userAuthServer.close();
      userAuthServer = null;
    }
  }

  private static ModifiableSolrParams getAuthParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(PseudoAuthenticator.USER_NAME, "user");
    return params;
  }

  @Test
  public void testCredentialsPassJobBased() throws Exception {
    // with jaas config
    Job job = new Job();
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try (HttpSolrClient jaasConfServer = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL)
        .withInvariantParams(getAuthParams())
        .build()) {
      JobSecurityUtil.initCredentials(jaasConfServer, job, zkHost);
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
      verifyCredentialsPass(job);
    } finally {
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    }

    // no jaas conf, but enabled via configuration
    job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    verifyCredentialsPass(job);
  }

  @Test
  public void testCredentialsPassFileBased() throws Exception {
    // get delegation token in a file.
    File dtFile = getCredentialsFile();
    Configuration conf = new Configuration();
    JobSecurityUtil.initCredentials(dtFile, conf, zkHost);
    verifyCredentialsPass(dtFile, conf);
  }

  @Test
  public void testCredentialsFailJobBased() throws Exception {
    // no jaas config, should fail
    Job job = new Job();
    JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    verifyCredentialsFail(job);

    // jaas config, but disabled via conf
    job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, false);
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try {
      JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
      verifyCredentialsFail(job);
    } finally {
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    }
  }

  @Test
  public void testCredentialsFailFileBased() throws Exception {
    // file is not correct format
    File dtFile = File.createTempFile(TestJobSecurityUtil.class.getName(), ".txt");
    Configuration conf = new Configuration();
    try {
      JobSecurityUtil.initCredentials(dtFile, conf, zkHost);
      fail ("Expected Exception");
    } catch (Exception e) { }

    // file valid, but disabled via conf
    dtFile = getCredentialsFile();
    conf.setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, false);
    JobSecurityUtil.initCredentials(dtFile, conf, zkHost);
    verifyCredentialsFail(conf);
  }

  @Test
  public void testBadArgsJobBased() throws Exception {
    Job job = new Job();
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    try {
      JobSecurityUtil.initCredentials(userAuthServer, job, null);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
    try {
      JobSecurityUtil.cleanupCredentials(null, job, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
  }

  @Test
  public void testBadArgsFileBased() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    File file = new File(".");
    try {
      JobSecurityUtil.initCredentials(file, conf, null);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
    try {
      JobSecurityUtil.cleanupCredentials(null, conf, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
  }

  private static interface CredentialsCleanup {
    void cleanupCredentials(SolrClient server) throws Exception;
  }

  private void verifyCredentialsPass(final Job job) throws Exception {
    // Ensure token is present in job credentials
    Token<? extends TokenIdentifier> authToken =
      job.getCredentials().getToken(new Text(zkHost));
    assertNotNull(authToken);

    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrClient server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, job, zkHost);
      }
    };
    verifyCredentialsPass(new String(authToken.getIdentifier(), "UTF-8"), cc);
  }

  private void verifyCredentialsPass(final File tokenFile, final Configuration conf)
  throws Exception {
    String authToken = getCredentialsString(tokenFile.getPath());
    assertNotNull(authToken);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrClient server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, conf, zkHost);
      }
    };
    verifyCredentialsPass(authToken, cc);
  }

  private void verifyCredentialsPass(String authToken, CredentialsCleanup cc) throws Exception {
    assertNotNull(authToken);

    HttpSolrClient server = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL)
                                                          .withDelegationToken(authToken)
                                                          .build();
    try {
      // Ensure a normal HttpSolrClient "just works"
      doSolrRequest(server, 200);

      // Cleanup credentials and verify server no longer works
      cc.cleanupCredentials(server);
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      server.close();
    }

    // a new server shouldn't work
    server = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL).build();
    try {
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);
    } finally {
      server.close();
    }

    // a new server shouldn't work even if we try to use the token
    server = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL)
                                           .withDelegationToken(authToken)
                                           .build();
    try {
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      server.close();
    }
  }

  private String getCredentialsString(String tokenFile) throws IOException {
    DelegationTokenRequest.Get getToken = new DelegationTokenRequest.Get();
    DelegationTokenResponse.Get getTokenResponse = new DelegationTokenResponse.Get();
    FileInputStream inputStream = new FileInputStream(tokenFile);
    try {
      NamedList<Object> response =
        getToken.getResponseParser().processResponse(inputStream, "UTF-8");
      getTokenResponse.setResponse(response);
      return getTokenResponse.getDelegationToken();
    } finally {
      inputStream.close();
    }
  }

  private void verifyCredentialsFail(final Job job) throws Exception {
    // Ensure token is not present
    Token<? extends TokenIdentifier> authToken =
      job.getCredentials().getToken(new Text(zkHost));
    assertNull(authToken);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrClient server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, job, zkHost);
      }
    };
    verifyCredentialsFail(cc);
  }

  private void verifyCredentialsFail(final Configuration conf) throws Exception {
    // Ensure file not present
    assertNull(conf.get(JobSecurityUtil.CREDENTIALS_FILE_LOCATION, null));
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrClient server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, conf, zkHost);
      }
    };
    verifyCredentialsFail(cc);
  }

  private void verifyCredentialsFail(CredentialsCleanup cc) throws Exception {
    HttpSolrClient server = (new HttpSolrClient.Builder()).withBaseSolrUrl(baseURL).build();

    try {
      // Ensure a normal HttpSolrClient doesn't work
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);

      cc.cleanupCredentials(server);
    } finally {
      server.close();
    }
  }

  private File getCredentialsFile() throws Exception {
    DelegationTokenRequest.Get getToken = new DelegationTokenRequest.Get();
    // create a response parser that maintains the raw response structure, so
    // we can dump the raw response to a file
    ResponseParser rawResponseParser = new ResponseParser() {
      @Override
      public String getWriterType() {
        return "raw";
      }

      @Override
      public NamedList<Object> processResponse(InputStream body, String encoding) {
        NamedList<Object> list = new NamedList<Object>();
        try {
          byte [] buffer = IOUtils.toByteArray(body);
          list.add("response", new String(buffer, encoding == null? "UTF-8": encoding));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return list;
      }

      @Override
      public NamedList<Object> processResponse(Reader reader) {
        throw new RuntimeException("Cannot handle character stream");
      }

      @Override
      public String getContentType() {
        return "application/json";
      }

      @Override
      public String getVersion() {
        return "1";
      }
    };
    getToken.setResponseParser(rawResponseParser);
    NamedList<Object> response = getToken.process(userAuthServer).getResponse();
    String tokenFileData = response.get("response").toString();
    File dtFile = File.createTempFile(TestJobSecurityUtil.class.getName(), ".txt");
    PrintWriter writer = new PrintWriter(dtFile, "UTF-8");
    try {
      writer.write(tokenFileData);
    } finally {
      writer.close();
    }
    return dtFile;
  }

  private void doSolrRequest(HttpSolrClient server, int expectedStatusCode)
  throws Exception {
    try {
      CoreAdminRequest request = new CoreAdminRequest() {
        @Override
        public SolrParams getParams() {
          return new ModifiableSolrParams();
        }
      };
      server.request(request);
      assertEquals(200, expectedStatusCode);
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }
}
