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

package org.apache.solr.handler.component;

import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.EnumSet;

public class QueryIndexAuthorizationComponent extends SearchComponent
{
  private static Logger log =
    LoggerFactory.getLogger(QueryIndexAuthorizationComponent.class);
  private SentryIndexAuthorizationSingleton sentryInstance;

  public QueryIndexAuthorizationComponent() {
    this(SentryIndexAuthorizationSingleton.getInstance());
  }

  @VisibleForTesting
  public QueryIndexAuthorizationComponent(SentryIndexAuthorizationSingleton sentryInstance) {
    super();
    this.sentryInstance = sentryInstance;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    sentryInstance.authorizeCollectionAction(
      rb.req, EnumSet.of(SearchModelAction.QUERY));
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Index Authorization";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
