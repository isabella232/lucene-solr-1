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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.common.util.NamedList;
import static org.apache.solr.servlet.SolrHadoopAuthenticationFilter.TOKEN_KIND;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for setting up MR Job Security Credentials
 */
public class JobSecurityUtil {

  private static final Logger LOG =
    LoggerFactory.getLogger(JobSecurityUtil.class);

  /**
   * Property in the job configuration that controls whether
   * secure credentials will be used.  If set explicitly, the value
   * is respected.  Otherwise, it is set to true in
   * {@link #initCredentials} if a JAAS configuration is configured
   * (via System property java.security.auth.login.config).
   */
  public static final String USE_SECURE_CREDENTIALS =
    "org.apache.solr.security.job.useSecureCredentials";

  /**
   * Location of the (optional) credentials file; if set,
   * the credentials file is used to pass the credentials
   * rather than the JobContext.
   */
  public static final String CREDENTIALS_FILE_LOCATION =
    "org.apache.solr.security.job.credentialFileLocation";

  /**
   * Init job credentials for when the tasks need to communicate with
   * secure solr directly.
   *
   * @param server SolrServer to get credentials from
   * @param job JobContext to store the credentials
   * @param serviceName service name for which the credentials are valid.  Should be a
   *        unique identifier for the solr cluster, i.e. the zkHost.
   */
  public static void initCredentials(SolrServer server, JobContext job, String serviceName)
  throws SolrServerException, IOException {
    verifyArgs(server, job, serviceName);
    Configuration conf = job.getConfiguration();
    if (conf.getBoolean(USE_SECURE_CREDENTIALS, false)
        || (isJaasConfigured() && conf.getBoolean(USE_SECURE_CREDENTIALS, true))) {
      LOG.info("Initializing job credentials");
      DelegationTokenRequest.Get getToken = new DelegationTokenRequest.Get();
      DelegationTokenResponse.Get getTokenResponse = getToken.process(server);
      String token = getTokenResponse.getDelegationToken();
      Token<? extends TokenIdentifier> credentialsToken = getCredentialsToken(token, serviceName);
      job.getCredentials().addToken(credentialsToken.getService(), credentialsToken);
      job.getConfiguration().setBooleanIfUnset(USE_SECURE_CREDENTIALS, true);
    } else {
      LOG.info("Skipping intitalization of job credentials");
    }
  }

  /**
   * Init job credentials from a file.  Currently used for Spark jobs, which can't
   * automatically pass the credentials in cluster mode.
   *
   * @param tokenFile file storing the tokens.  File should be the output of a curl
   *        call to get the token, e.g.
   *        "curl --negotiate -u: 'http://host:port/solr/?op=GETDELEGATIONTOKEN'"
   * @param conf configuration passed to MR/Spark job.
   * @param serviceName service name for which the credentials are valid.  Should be a
   *        unique identifier for the solr cluster, i.e. the zkHost.
   */
  public static void initCredentials(File tokenFile, Configuration conf, String serviceName)
  throws IOException {
    verifyArgs(tokenFile, conf, serviceName);
    if (conf.getBoolean(USE_SECURE_CREDENTIALS, true)) {
      LOG.info("Initializing job credentials");
      // check that the value in the file is reasonable so we
      // can throw an exception before kicking off the job
      getCredentialsString(tokenFile.getPath());
      conf.set(CREDENTIALS_FILE_LOCATION, tokenFile.getPath());
      conf.setBooleanIfUnset(USE_SECURE_CREDENTIALS, true);
    } else {
      LOG.info("Skipping intitalization of job credentials");
    }
  }

  /**
   * Load job credentials for Solr clients running in this process.  Clients created after
   * calling this method will automatically authenticate using the provided credentials.
   *
   * @param job JobContext to load the credentials from
   * @param serviceName should be the same as passed to {@link #initCredentials}
   */
  public static void loadCredentialsForClients(JobContext job, String serviceName)
  throws IOException {
    verifyArgs(job, serviceName);
    if (job.getConfiguration().getBoolean(USE_SECURE_CREDENTIALS, false)) {
      LOG.info("Loading job credentials for clients");
      System.setProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY, getCredentialsString(job, serviceName));
    }
  }

  /**
   * Load job credentials for Solr clients running in this process.  Clients created after
   * calling this method will automatically authenticate using the provided credentials.
   *
   * @param conf Configuration that stores the CREDENTIALS_FILE_LOCATION, probably
   *        passed to {@link #initCredentials(File, Configuration)}
   * @param serviceName should be the same as passed to {@link #initCredentials}
   */
  public static void loadCredentialsForClients(Configuration conf, String serviceName)
  throws IOException {
    verifyArgs(conf, serviceName);
    if (conf.getBoolean(USE_SECURE_CREDENTIALS, false)) {
      String credentialsFile = conf.get(CREDENTIALS_FILE_LOCATION);
      if (credentialsFile != null) {
        LOG.info("Loading job credentials for clients");
        String credentialsString = getCredentialsString(credentialsFile);
        System.setProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY, credentialsString);
      }
    }
  }

  /**
   * Cleanup job credentials; call at the end of the job so that credentials
   * can't be used outside of the lifetime of the job.
   *
   * @param server SolrServer to cleanup the credentials on
   * @param job JobContext to load the credentials from
   * @param serviceName should be the same as passed to {@link #initCredentials}
   */
  public static void cleanupCredentials(SolrServer server, JobContext job, String serviceName)
  throws SolrServerException, IOException {
    verifyArgs(server, job, serviceName);
    if (job.getConfiguration().getBoolean(USE_SECURE_CREDENTIALS, false)) {
      cancelCredentials(server, getCredentialsString(job, serviceName));
    }
  }


  /**
   * Cleanup job credentials; call at the end of the job so that credentials
   * can't be used outside of the lifetime of the job.
   *
   * @param server SolrServer to cleanup the credentials on
   * @param conf Configuration that stores the CREDENTIALS_FILE_LOCATION, probably
   *        passed to {@link #initCredentials(File, Configuration)}
   * @param serviceName should be the same as passed to {@link #initCredentials}
   */
  public static void cleanupCredentials(SolrServer server, Configuration conf, String serviceName)
  throws SolrServerException, IOException {
    verifyArgs(server, conf, serviceName);
     if (conf.getBoolean(USE_SECURE_CREDENTIALS, false)) {
      String credentialsFile = conf.get(CREDENTIALS_FILE_LOCATION);
      if (credentialsFile != null) {
        cancelCredentials(server, getCredentialsString(credentialsFile));
      }
    }
  }

  private static void cancelCredentials(SolrServer server, String credentialsString)
  throws SolrServerException, IOException {
    LOG.info("Cleaning up job credentials");
    DelegationTokenRequest.Cancel cancelToken =
      new DelegationTokenRequest.Cancel(credentialsString);
    cancelToken.process(server);
  }

  private static boolean isJaasConfigured() {
    return System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP) != null;
  }

  private static Token<? extends TokenIdentifier> getCredentialsToken(String token, String serviceName) throws IOException {
    // only need to reconstruct the identifier in order to use it, so just make the password empty.
    return new Token<SolrTokenIdentifier>(
      token.getBytes(StandardCharsets.UTF_8), new byte[0], new Text(TOKEN_KIND), new Text(serviceName));
  }

  private static void verifyArgs(SolrServer server, JobContext job, String serviceName) {
    verifyArgs(job, serviceName);
    if (server == null) {
      throw new IllegalArgumentException("server must be non-null");
    }
  }

  private static void verifyArgs(JobContext job, String serviceName) {
    if (job == null) {
      throw new IllegalArgumentException("job must be non-null");
    }
    if (serviceName == null) {
      throw new IllegalArgumentException("serviceName must be non-null");
    }
  }

  private static void verifyArgs(File tokenFile, Configuration conf, String serviceName) {
    verifyArgs(conf, serviceName);
    if (tokenFile == null) {
      throw new IllegalArgumentException("tokenFile must be non-null");
    }
  }

  private static void verifyArgs(SolrServer server, Configuration conf, String serviceName) {
    verifyArgs(conf, serviceName);
    if (server == null) {
      throw new IllegalArgumentException("server must be non-null");
    }
  }

  private static void verifyArgs(Configuration conf, String serviceName) {
    if (conf == null) {
      throw new IllegalArgumentException("conf must be non-null");
    }
    if (serviceName == null) {
      throw new IllegalArgumentException("serviceName must be non-null");
    }
  }

  private static String getCredentialsString(JobContext job, String serviceName) throws IOException {
    Token<? extends TokenIdentifier> token = job.getCredentials().getToken(new Text(serviceName));
    if (token == null) {
      throw new IOException("Unable to locate credentials");
    }
    return new String(token.getIdentifier(), StandardCharsets.UTF_8);
  }

  private static String getCredentialsString(String tokenFile) throws IOException {
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

  private static class SolrTokenIdentifier extends AbstractDelegationTokenIdentifier {
    public Text getKind() {
      return new Text(TOKEN_KIND);
    }
  }
}
