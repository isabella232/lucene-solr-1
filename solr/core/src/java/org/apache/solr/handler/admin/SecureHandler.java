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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.zookeeper.KeeperException;

/**
 * Container class for Secure (sentry-aware) versions of handlers
 */
public class SecureHandler {

  public static final Set<SearchModelAction> QUERY_ONLY = EnumSet.of(SearchModelAction.QUERY);
  public static final Set<SearchModelAction> UPDATE_ONLY = EnumSet.of(SearchModelAction.UPDATE);
  public static final Set<SearchModelAction> QUERY_AND_UPDATE = EnumSet.of(SearchModelAction.QUERY, SearchModelAction.UPDATE);

  // Hack to provide a test-only version of SentryIndexAuthorizationSingleton
  public static SentryIndexAuthorizationSingleton testOverride = null;

  /**
   * Check with sentry whether all of the specified actions are valid for the request
   */
  public static void checkSentry(SolrQueryRequest req, Set<SearchModelAction> andActions) {
   // Sentry currently does have AND support for actions; need to check
    // actions one at a time
    final SentryIndexAuthorizationSingleton sentryInstance =
      (testOverride == null)?SentryIndexAuthorizationSingleton.getInstance():testOverride;
    for (SearchModelAction action : andActions) {
      sentryInstance.authorizeAdminAction(
        req, EnumSet.of(action));
    }
  }

  public static class SecureLoggingHandler extends LoggingHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      // logging handler can be used both to read and change logs
      SecureHandler.checkSentry(req, QUERY_AND_UPDATE);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureLukeRequestHandler extends LukeRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecurePluginInfoHandler extends PluginInfoHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecurePropertiesRequestHandler extends PropertiesRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureShowFileRequestHandler extends ShowFileRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
     throws IOException, KeeperException, InterruptedException {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureSolrInfoMBeanHandler extends SolrInfoMBeanHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureSystemInfoHandler extends SystemInfoHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureThreadDumpHandler extends ThreadDumpHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
      SecureHandler.checkSentry(req, QUERY_ONLY);
      super.handleRequestBody(req, rsp);
    }
  }
}
