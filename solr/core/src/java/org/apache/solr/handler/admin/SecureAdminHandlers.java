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

  @Override
  protected StandardHandler[] getStandardHandlers() {
    return new StandardHandler[] {
      new StandardHandler( "luke", new SecureHandler.SecureLukeRequestHandler() ),
      new StandardHandler( "system", new SecureHandler.SecureSystemInfoHandler() ),
      new StandardHandler( "mbeans", new SecureHandler.SecureSolrInfoMBeanHandler() ),
      new StandardHandler( "plugins", new SecureHandler.SecurePluginInfoHandler() ),
      new StandardHandler( "threads", new SecureHandler.SecureThreadDumpHandler() ),
      new StandardHandler( "properties", new SecureHandler.SecurePropertiesRequestHandler() ),
      new StandardHandler( "logging", new SecureHandler.SecureLoggingHandler() ),
      new StandardHandler( "file", new SecureHandler.SecureShowFileRequestHandler() )
    };
  }
}
