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
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster;
import static org.apache.solr.cloud.HttpParamDelegationTokenMiniSolrCloudCluster.USER_PARAM;
import org.apache.solr.cloud.MiniSolrCloudCluster;
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
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.apache.solr.SolrTestCaseJ4.SuppressSSL
public class TestJobSecurityUtil extends SolrTestCaseJ4 {
  private static Logger log = LoggerFactory.getLogger(TestJobSecurityUtil.class);
  private static final int NUM_SERVERS = 2;
  private static MiniSolrCloudCluster miniCluster;
  private static HttpSolrServer userAuthServer;
  private static String baseURL;
  private static String zkHost;

  @BeforeClass
  public static void startup() throws Exception {
    File solrXml = SolrTestCaseJ4.getFile("solr/solr-no-core.xml");
    miniCluster = new HttpParamDelegationTokenMiniSolrCloudCluster(NUM_SERVERS, null,
      solrXml, null);
    JettySolrRunner runner = miniCluster.getJettySolrRunners().get(0);
    baseURL = runner.getBaseUrl().toString();
    userAuthServer = new UserAuthHttpSolrServer(baseURL);
    zkHost = miniCluster.getZkServer().getZkAddress();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    userAuthServer.shutdown();
  }

  @Test
  public void testCredentialsPassJobBased() throws Exception {
    // with jaas config
    Job job = new Job();
    // enable credentials, since disabled by default
    job.getConfiguration().setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try {
      // get a new UserAuthSolrServer for the jaas conf
      HttpSolrServer jaasConfServer = new UserAuthHttpSolrServer(baseURL);
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
    // enable credentials, since disabled by default
    conf.setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
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
    // enable credentials, since disabled by default
    conf.setBoolean(JobSecurityUtil.USE_SECURE_CREDENTIALS, true);
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
      Job nullJob = null;
      JobSecurityUtil.loadCredentialsForClients(nullJob, zkHost);
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
      Configuration nullConf = null;
      JobSecurityUtil.loadCredentialsForClients(nullConf, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
    try {
      JobSecurityUtil.cleanupCredentials(null, conf, zkHost);
      fail ("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex) {}
  }

  @Test
  public void testDisabledByDefault() throws Exception {
    // Job based
    Job job = new Job();
    JobSecurityUtil.initCredentials(userAuthServer, job, zkHost);
    assertEquals(0, job.getCredentials().numberOfTokens());

    // File based
    Configuration conf = new Configuration();
    File file = new File(".");
    JobSecurityUtil.initCredentials(file, conf, zkHost);
    assertNull(conf.get(JobSecurityUtil.CREDENTIALS_FILE_LOCATION));
  }

  private static interface CredentialsCleanup {
    void cleanupCredentials(SolrServer server) throws Exception;
  }

  private void verifyCredentialsPass(final Job job) throws Exception {
    // Ensure token is present in job credentials
    Token<? extends TokenIdentifier> authToken =
      job.getCredentials().getToken(new Text(zkHost));
    assertNotNull(authToken);
    JobSecurityUtil.loadCredentialsForClients(job, zkHost);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrServer server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, job, zkHost);
      }
    };
    verifyCredentialsPass(new String(authToken.getIdentifier(), "UTF-8"), cc);
  }

  private void verifyCredentialsPass(final File tokenFile, final Configuration conf)
  throws Exception {
    String authToken = getCredentialsString(tokenFile.getPath());
    assertNotNull(authToken);
    JobSecurityUtil.loadCredentialsForClients(conf, zkHost);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrServer server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, conf, zkHost);
      }
    };
    verifyCredentialsPass(authToken, cc);
  }

  private void verifyCredentialsPass(String authToken, CredentialsCleanup cc) throws Exception {
    String token = System.getProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
    assertNotNull(token);
    assertEquals(token, authToken);

    HttpSolrServer server = new HttpSolrServer(baseURL);
    try {
      // Ensure a normal HttpSolrServer "just works"
      doSolrRequest(server, 200);

      // Cleanup credentials and verify server no longer works
      cc.cleanupCredentials(server);
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      System.clearProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
      server.shutdown();
    }

    // a new server shouldn't work
    server = new HttpSolrServer(baseURL);
    try {
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);
    } finally {
      server.shutdown();
    }

    // a new server shouldn't work even if we try to use the token
    server = new TokenAuthHttpSolrServer(baseURL, token);
    try {
      doSolrRequest(server, ErrorCode.FORBIDDEN.code);
    } finally {
      server.shutdown();
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
    JobSecurityUtil.loadCredentialsForClients(job, zkHost);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrServer server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, job, zkHost);
      }
    };
    verifyCredentialsFail(cc);
  }

  private void verifyCredentialsFail(final Configuration conf) throws Exception {
    // Ensure file not present
    assertNull(conf.get(JobSecurityUtil.CREDENTIALS_FILE_LOCATION, null));
    JobSecurityUtil.loadCredentialsForClients(conf, zkHost);
    CredentialsCleanup cc = new CredentialsCleanup() {
      @Override
      public void cleanupCredentials(SolrServer server) throws Exception {
        JobSecurityUtil.cleanupCredentials(server, conf, zkHost);
      }
    };
    verifyCredentialsFail(cc);
  }

  private void verifyCredentialsFail(CredentialsCleanup cc) throws Exception {
    String token = System.getProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY);
    assertNull(token);

    HttpSolrServer server = new HttpSolrServer(baseURL);
    try {
      // Ensure a normal HttpSolrServer doesn't work
      doSolrRequest(server, ErrorCode.UNAUTHORIZED.code);

      cc.cleanupCredentials(server);
    } finally {
      server.shutdown();
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
          list.add("response", new String(buffer,encoding));
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

  private void doSolrRequest(HttpSolrServer server, int expectedStatusCode)
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
    } catch (HttpSolrServer.RemoteSolrException ex) {
      assertEquals(expectedStatusCode, ex.code());
    }
  }

  /**
   * HttpSolrServer that automatically authenticates via USER_PARAM
   */
  private static class UserAuthHttpSolrServer extends HttpSolrServer {
    public UserAuthHttpSolrServer(String baseURL) {
      super(baseURL);
      invariantParams = new ModifiableSolrParams();
      invariantParams.set(USER_PARAM, "user");
    }
  }

  /**
   * HttpSolrServer that automatically authenticates via the delegation token
   */
  private static class TokenAuthHttpSolrServer extends HttpSolrServer {
    private TokenAuthHttpSolrServer(String baseURL, String token) {
      super(baseURL);
      invariantParams = new ModifiableSolrParams();
      invariantParams.set(HttpSolrServer.DELEGATION_TOKEN_PARAM, token);
    }
  }
}
