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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.solr.request.LocalSolrQueryRequest;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.net.URLEncoder;

public class QueryDocAuthorizationComponent extends SearchComponent
{
  private static Logger log =
    LoggerFactory.getLogger(QueryDocAuthorizationComponent.class);
  public static String AUTH_FIELD_PROP = "sentryAuthField";
  public static String DEFAULT_AUTH_FIELD = "_sentry_auth_";
  private SentryIndexAuthorizationSingleton sentryInstance;
  private String authField;

  public QueryDocAuthorizationComponent() {
    this(SentryIndexAuthorizationSingleton.getInstance());
  }

  @VisibleForTesting
  public QueryDocAuthorizationComponent(SentryIndexAuthorizationSingleton sentryInstance) {
    super();
    this.sentryInstance = sentryInstance;
  }

  @Override
  public void init(NamedList args) {
    Object fieldArg = args.get(AUTH_FIELD_PROP);
    this.authField = (fieldArg == null) ? DEFAULT_AUTH_FIELD : fieldArg.toString();
    log.info("QueryDocAuthorizationComponent authField: " + this.authField);
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    String userName = sentryInstance.getUserName(rb.req);
    String superUser = (System.getProperty("solr.authorization.superuser", "solr"));
    if (superUser.equals(userName)) {
      return;
    }
    List<String> groups = sentryInstance.getGroups(userName);
    if (groups != null && groups.size() > 0) {
      StringBuilder builder = new StringBuilder();
      builder.append(authField).append(":(");
      for (int i = 0; i < groups.size(); ++i) {
        if (i != 0) {
           builder.append(" OR ");
        }
        builder.append(groups.get(i));
      }
      builder.append(")");
      ModifiableSolrParams newParams = new ModifiableSolrParams(rb.req.getParams());
      String result = builder.toString();
      newParams.add("fq", result);
      rb.req.setParams(newParams);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Document Authorization";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
