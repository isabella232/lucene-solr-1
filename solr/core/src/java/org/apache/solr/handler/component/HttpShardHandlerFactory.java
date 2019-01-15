package org.apache.solr.handler.component;
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


import com.google.common.annotations.VisibleForTesting;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized {
  protected static Logger log = LoggerFactory.getLogger(HttpShardHandlerFactory.class);
  private static final String DEFAULT_SCHEME = "http";
  
  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  private ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<Runnable>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("httpShardExecutor")
  );

  protected HttpClient defaultClient;
  private LBHttpSolrServer loadbalancer;
  //default values:
  int soTimeout = 0; 
  int connectionTimeout = 0; 
  int maxConnectionsPerHost = 20;
  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  boolean accessPolicy = false;
  boolean useRetries = false;
  private WhitelistHostChecker whitelistHostChecker = null;

  private String scheme = null;

  private final Random r = new Random();

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";
  
  // Turn on retries for certain IOExceptions, many of which can happen
  // due to connection pooling limitations / races
  static final String USE_RETRIES = "useRetries";

  public static final String INIT_SHARDS_WHITELIST = "shardsWhitelist";

  static final String INIT_SOLR_DISABLE_SHARDS_WHITELIST = "solr.disable." + INIT_SHARDS_WHITELIST;

  static final String SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE = " set -D"+INIT_SOLR_DISABLE_SHARDS_WHITELIST+"=true to disable shards whitelist checks";

  /**
   * Get {@link ShardHandler} that uses the default http client.
   */
  @Override
  public ShardHandler getShardHandler() {
    return getShardHandler(defaultClient);
  }

  /**
   * Get {@link ShardHandler} that uses custom http client.
   */
  public ShardHandler getShardHandler(final HttpClient httpClient){
    return new HttpShardHandler(this, httpClient);
  }

  /**
   * Returns this Factory's {@link WhitelistHostChecker}.
   * This method can be overridden to change the checker implementation.
   */
  public WhitelistHostChecker getWhitelistHostChecker() {
    return this.whitelistHostChecker;
  }

  @Deprecated // For temporary use by the TermsComponent only.
  static boolean doGetDisableShardsWhitelist() {
    return getDisableShardsWhitelist();
  }


  private static boolean getDisableShardsWhitelist() {
    return Boolean.getBoolean(INIT_SOLR_DISABLE_SHARDS_WHITELIST);
  }

  @Override
  public void init(PluginInfo info) {
    NamedList args = info.initArgs;
    this.soTimeout = getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT, soTimeout);
    this.scheme = getParameter(args, INIT_URL_SCHEME, null);
    if(StringUtils.endsWith(this.scheme, "://")) {
      this.scheme = StringUtils.removeEnd(this.scheme, "://");
    }
    this.connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
    this.maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, accessPolicy);
    this.useRetries = getParameter(args, USE_RETRIES, useRetries);
    this.whitelistHostChecker = new WhitelistHostChecker(args == null? null: (String) args.get(INIT_SHARDS_WHITELIST), !getDisableShardsWhitelist());
    log.info("Host whitelist initialized: {}", this.whitelistHostChecker);

    
    // magic sysprop to make tests reproducible: set by SolrTestCaseJ4.
    String v = System.getProperty("tests.shardhandler.randomSeed");
    if (v != null) {
      r.setSeed(Long.parseLong(v));
    }

    BlockingQueue<Runnable> blockingQueue = (this.queueSize == -1) ?
        new SynchronousQueue<Runnable>(this.accessPolicy) :
        new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy);

    this.commExecutor = new ThreadPoolExecutor(
        this.corePoolSize,
        this.maximumPoolSize,
        this.keepAliveTime, TimeUnit.SECONDS,
        blockingQueue,
        new DefaultSolrThreadFactory("httpShardExecutor")
    );

    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 10000);
    clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, soTimeout);
    clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
    if (!useRetries) {
      clientParams.set(HttpClientUtil.PROP_USE_RETRY, false);
    }
    this.defaultClient = HttpClientUtil.createClient(clientParams);
    
    // must come after createClient
    if (useRetries) {
      // our default retry handler will never retry on IOException if the request has been sent already,
      // but for these read only requests we can use the standard DefaultHttpRequestRetryHandler rules
      ((DefaultHttpClient) this.defaultClient).setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler());
    }
    
    this.loadbalancer = createLoadbalancer(defaultClient);
  }

  protected ThreadPoolExecutor getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrServer createLoadbalancer(HttpClient httpClient){
    return new LBHttpSolrServer(httpClient);
  }

  protected <T> T getParameter(NamedList initArgs, String configKey, T defaultValue) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    log.info("Setting {} to: {}", configKey, toReturn);
    return toReturn;
  }


  @Override
  public void close() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
    } finally {
      try {
        if (defaultClient != null) {
          defaultClient.getConnectionManager().shutdown();
        }
      } finally {
        
        if (loadbalancer != null) {
          loadbalancer.shutdown();
        }
      }
    }
  }

  /**
   * Makes a request to one or more of the given urls, using the configured load balancer.
   *
   * @param req The solr search request that should be sent through the load balancer
   * @param urls The list of solr server urls to load balance across
   * @return The response from the request
   */
  public LBHttpSolrServer.Rsp makeLoadBalancedRequest(final QueryRequest req, List<String> urls)
    throws SolrServerException, IOException {
    return loadbalancer.request(new LBHttpSolrServer.Req(req, urls));
  }

  /**
   * Creates a randomized list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> makeURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i=0; i<urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    //
    // Shuffle the list instead of use round-robin by default.
    // This prevents accidental synchronization where multiple shards could get in sync
    // and query the same replica at the same time.
    //
    if (urls.size() > 1)
      Collections.shuffle(urls, r);

    return urls;
  }

  /**
   * Creates a new completion service for use by a single set of distributed requests.
   */
  public CompletionService newCompletionService() {
    return new ExecutorCompletionService<ShardResponse>(commExecutor);
  }
  
  /**
   * Rebuilds the URL replacing the URL scheme of the passed URL with the
   * configured scheme replacement.If no scheme was configured, the passed URL's
   * scheme is left alone.
   */
  private String buildUrl(String url) {
    if(!URLUtil.hasScheme(url)) {
      return StringUtils.defaultIfEmpty(scheme, DEFAULT_SCHEME) + "://" + url;
    } else if(StringUtils.isNotEmpty(scheme)) {
      return scheme + "://" + URLUtil.removeScheme(url);
    }
    
    return url;
  }
  
  /**
   * Class used to validate the hosts in the "shards" parameter when doing a distributed
   * request
   */
  public static class WhitelistHostChecker {
    
    /**
     * List of the whitelisted hosts. Elements in the list will be host:port (no protocol or context)
     */
    private final Set<String> whitelistHosts;
    
    /**
     * Indicates whether host checking is enabled 
     */
    private final boolean whitelistHostCheckingEnabled;
    
    public WhitelistHostChecker(String whitelistStr, boolean enabled) {
      this.whitelistHosts = implGetShardsWhitelist(whitelistStr);
      this.whitelistHostCheckingEnabled = enabled;
    }
    
    final static Set<String> implGetShardsWhitelist(final String shardsWhitelist) {
      if (shardsWhitelist != null && !shardsWhitelist.isEmpty()) {
        List<String> shardList = StrUtils.splitSmart(shardsWhitelist, ',');
        Set<String> result = new HashSet<>();
        for(String shard : shardList) {
          String hostUrl =  shard.trim();
          URL url;
          try {
            if (!hostUrl.startsWith("http://") && !hostUrl.startsWith("https://")) {
              // It doesn't really matter which protocol we set here because we are not going to use it. We just need a full URL.
              url = new URL("http://" + hostUrl);
            } else {
              url = new URL(hostUrl);
            }
          } catch (MalformedURLException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid URL syntax in \"" + INIT_SHARDS_WHITELIST + "\": " + shardsWhitelist, e);
          }
          if (url.getHost() == null || url.getPort() < 0) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid URL syntax in \"" + INIT_SHARDS_WHITELIST + "\": " + shardsWhitelist);
          }
          result.add(url.getHost() + ":" + url.getPort());
        }
        return result;
      }
      return null;
    }
    
    
    /**
     * @see #checkWhitelist(ClusterState, String, List)
     */
    protected void checkWhitelist(String shardsParamValue, List<String> shardUrls) {
      checkWhitelist(null, shardsParamValue, shardUrls);
    }
    
    /**
     * Checks that all the hosts for all the shards requested in shards parameter exist in the configured whitelist
     * or in the ClusterState (in case of cloud mode)
     * 
     * @param clusterState The up to date ClusterState, can be null in case of non-cloud mode
     * @param shardsParamValue The original shards parameter
     * @param shardUrls The list of cores generated from the shards parameter. 
     */
    protected void checkWhitelist(ClusterState clusterState, String shardsParamValue, List<String> shardUrls) {
      if (!whitelistHostCheckingEnabled) {
        return;
      }
      Set<String> localWhitelistHosts;
      if (whitelistHosts == null && clusterState != null) {
        // TODO: We could implement caching, based on the version of the live_nodes znode
        localWhitelistHosts = generateWhitelistFromLiveNodes(clusterState);
      } else if (whitelistHosts != null) {
        localWhitelistHosts = whitelistHosts;
      } else {
        localWhitelistHosts = Collections.emptySet();
      }

      for(String string : shardUrls){
        String shardUrl =  string.trim();
        URL url;
        try {
          if (!shardUrl.startsWith("http://") && !shardUrl.startsWith("https://")) {
            // It doesn't really matter which protocol we set here because we are not going to use it. We just need a full URL.
            url = new URL("http://" + shardUrl);
          } else {
            url = new URL(shardUrl);
          }
        } catch (MalformedURLException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid URL syntax in \"shards\" parameter: " + shardsParamValue, e);
        }
        if (url.getHost() == null || url.getPort() < 0) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid URL syntax in \"shards\" parameter: " + shardsParamValue);
        }
        if (!localWhitelistHosts.contains(url.getHost() + ":" + url.getPort())) {
          log.warn("The '"+ShardParams.SHARDS+"' parameter value '"+shardsParamValue+"' contained value(s) not on the shards whitelist ("+localWhitelistHosts+"), shardUrl:" + shardUrl);
          throw new SolrException(ErrorCode.FORBIDDEN,
              "The '"+ShardParams.SHARDS+"' parameter value '"+shardsParamValue+"' contained value(s) not on the shards whitelist. shardUrl:" + shardUrl + "." +
                  HttpShardHandlerFactory.SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE);
        }
      }
    }
    
    Set<String> generateWhitelistFromLiveNodes(ClusterState clusterState) {
      Set<String> liveNodes = clusterState.getLiveNodes();
      Set<String> result = new HashSet<>();
      for(String liveNode : liveNodes) {
        result.add(liveNode.substring(0, liveNode.indexOf('_')));
      }
      return result;
    }
    
    public boolean hasExplicitWhitelist() {
      return this.whitelistHosts != null;
    }
    
    public boolean isWhitelistHostCheckingEnabled() {
      return whitelistHostCheckingEnabled;
    }
    
    /**
     * Only to be used by tests
     */
    @VisibleForTesting
    Set<String> getWhitelistHosts() {
      return this.whitelistHosts;
    }

    @Override
    public String toString() {
      return "WhitelistHostChecker [whitelistHosts=" + whitelistHosts + ", whitelistHostCheckingEnabled="
          + whitelistHostCheckingEnabled + "]";
    }
    
  }
  
}
