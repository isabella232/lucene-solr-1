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
import java.net.URL;
import java.util.EnumSet;
import java.util.Set;
import org.apache.sentry.core.model.search.SearchModelAction;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;

/**
 * Secure (integrated with sentry) wrapper for Admin Request Handlers.
 */
public class SecureAdminHandlerWrapper extends RequestHandlerBase {
  private RequestHandlerBase nonSecureHandler;
  private Set<SearchModelAction> andActions;

  /**
   * @param nonSecureHandler the request handler to wrap
   * @param andActions the actions to check.  Uses AND semantics, i.e.
   * all actions must pass for the action to complete
   */
  public SecureAdminHandlerWrapper(RequestHandlerBase nonSecureHandler,
      Set<SearchModelAction> andActions) {
    this.nonSecureHandler = nonSecureHandler;
    this.andActions = andActions;
  }

  @Override
  public void init(NamedList args) {
    nonSecureHandler.init(args);
  }

  @Override
  public NamedList getInitArgs() {
    return nonSecureHandler.getInitArgs();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Sentry currently does have AND support for actions; need to check
    // actions one at a time
    for (SearchModelAction action : andActions) {
      SentryIndexAuthorizationSingleton.getInstance().authorizeCollectionAction(req, EnumSet.of(action));
    }
    nonSecureHandler.handleRequestBody(req, rsp);
  }

  @Override
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    nonSecureHandler.handleRequest(req, rsp);
  }

  @Override
  public String getName() {
    return nonSecureHandler.getName();
  }

  @Override
  public String getDescription() {
    return "Secure Version: " + nonSecureHandler.getDescription();
  }

  @Override
  public String getSource() {
    return nonSecureHandler.getSource();
  }

  @Override
  public String getVersion() {
    return nonSecureHandler.getVersion();
  }

  @Override
  public Category getCategory() {
    return nonSecureHandler.getCategory();
  }

  @Override
  public URL[] getDocs() {
    return nonSecureHandler.getDocs();
  }

  @Override
  public NamedList<Object> getStatistics() {
    return nonSecureHandler.getStatistics();
  }
}
