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
package org.apache.solr.handler;

import org.apache.sentry.binding.solr.authz.SentrySolrPluginImpl;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryDocAuthorizationComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * This class extends {@linkplain SQLHandler} functionality such that in case
 * the collection/core is configured with document-level security, this functionality
 * can be disabled.
 */
public class SentrySQLHandler extends SQLHandler {
  private static final boolean enableSqlQuery = Boolean.getBoolean("solr.sentry.enableSqlQuery");
  private volatile boolean disableSqlQuery = false;

  @Override
  public void inform(SolrCore core) {
    super.inform(core);
    this.disableSqlQuery = shouldDisable(core);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (disableSqlQuery) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "This core is configured with document level security."
          + " Currently sql handler does not support document-level security.");
    }
    super.handleRequestBody(req, rsp);
  }

  private boolean shouldDisable(SolrCore core) {
    if (!enableSqlQuery && core.getCoreContainer().getAuthorizationPlugin() instanceof SentrySolrPluginImpl) {
      return (core.getSolrConfig().getNode(
          "searchComponent[@class=\""+ QueryDocAuthorizationComponent.class.getName()+"\"]", false) != null);
    }

    return false;
  }
}
