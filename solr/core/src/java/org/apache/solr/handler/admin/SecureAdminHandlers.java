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

import java.util.EnumSet;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.handler.RequestHandlerBase;

/**
 * Secure version of AdminHandlers that creates Sentry-aware AdminHandlers.
 */
public class SecureAdminHandlers extends AdminHandlers {

  private StandardHandler createStandardHandler(String name, RequestHandlerBase requestHandler,
      EnumSet<SearchModelAction> actions) {
    return new StandardHandler(name, new SecureAdminHandlerWrapper(requestHandler, actions));
  }

  @Override
  protected StandardHandler[] getStandardHandlers() {
    EnumSet<SearchModelAction> queryOnly = EnumSet.of(SearchModelAction.QUERY);
    EnumSet<SearchModelAction> queryAndUpdate =
      EnumSet.of(SearchModelAction.QUERY, SearchModelAction.UPDATE);

    return new StandardHandler[] {
      createStandardHandler( "luke", new LukeRequestHandler(), queryOnly ),
      createStandardHandler( "system", new SystemInfoHandler(), queryOnly ),
      createStandardHandler( "mbeans", new SolrInfoMBeanHandler(), queryOnly ),
      createStandardHandler( "plugins", new PluginInfoHandler(), queryOnly ),
      createStandardHandler( "threads", new ThreadDumpHandler(), queryOnly ),
      createStandardHandler( "properties", new PropertiesRequestHandler(), queryOnly ),
      // user has the ability to change what is logged via LoggingHandler
      createStandardHandler( "logging", new LoggingHandler(), queryAndUpdate ),
      createStandardHandler( "file", new ShowFileRequestHandler(), queryOnly )
    };
  }
}
