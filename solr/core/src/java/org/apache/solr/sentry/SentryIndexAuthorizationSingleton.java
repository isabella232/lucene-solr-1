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
package org.apache.solr.sentry;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;
import org.apache.sentry.binding.solr.authz.SolrAuthzBinding;
import org.apache.sentry.binding.solr.authz.SentrySolrAuthorizationException;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.net.URL;
import javax.servlet.http.HttpServletRequest;

public class SentryIndexAuthorizationSingleton {

  private static Logger log =
    LoggerFactory.getLogger(SentryIndexAuthorizationSingleton.class);

  private static final SentryIndexAuthorizationSingleton INSTANCE =
    new SentryIndexAuthorizationSingleton();

  private final SolrAuthzBinding binding;

  private SentryIndexAuthorizationSingleton() {
    SolrAuthzBinding tmpBinding = null;
    String propertyName = "solr.authorization.sentry.site";
    String sentrySiteLocation = System.getProperty(propertyName);
    try {
      if (sentrySiteLocation != null && sentrySiteLocation != "") {
        tmpBinding =
          new SolrAuthzBinding(new SolrAuthzConf(new URL("file://" + sentrySiteLocation)));
        log.info("SolrAuthzBinding created successfully");
      } else {
        log.info("SolrAuthzBinding not created because " + propertyName
          + " not set, sentry not enabled");
      }
    } catch (Exception ex) {
      log.error("Unable to create SolrAuthzBinding", ex);
    }
    binding = tmpBinding;
  }

  public static SentryIndexAuthorizationSingleton getInstance() {
    return INSTANCE;
  }

  /**
   * Returns true iff Sentry index authorization checking is enabled
   */
  public boolean isEnabled() {
    return binding != null;
  }

  public void authorizeAdminAction(SolrQueryRequest req,
      Set<SearchModelAction> actions) throws SolrException {
    authorizeCollectionAction(req, actions, "admin");
    authorizeCollectionAction(req, actions, null);
  }

  public void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions) throws SolrException {
    authorizeCollectionAction(req, actions, null);
  }

  private void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, String collectionName) {
    String exMsg = null;

    if (binding == null) {
      exMsg = "Solr binding was not created successfully.  Defaulting to no access";
    }
    else {
      SolrCore solrCore = req.getCore();
      HttpServletRequest httpServletRequest = (HttpServletRequest)req.getContext().get("httpRequest");
      if (httpServletRequest == null) {
        StringBuilder builder = new StringBuilder("Unable to locate HttpServletRequest");
        if (solrCore.getSolrConfig().getBool(
          "requestDispatcher/requestParsers/@addHttpRequestToContext", true) == false) {
          builder.append(", ensure requestDispatcher/requestParsers/@addHttpRequestToContext is set to true");
        }
        exMsg = builder.toString();
      }
      else {
        String userName = (String)httpServletRequest.getAttribute(SolrHadoopAuthenticationFilter.USER_NAME);
        Subject subject = new Subject(userName);
        Subject superUser = new Subject(System.getProperty("solr.authorization.superuser", "solr"));
        if (collectionName == null) {
          collectionName = solrCore.getCoreDescriptor().getCloudDescriptor().getCollectionName();
        }
        Collection collection = new Collection(collectionName);
        try {
          if (!superUser.getName().equals(subject.getName())) {
            binding.authorizeCollection(subject, collection, actions);
          }
        } catch (SentrySolrAuthorizationException ex) {
          throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, ex);
        }
      }
    }
    if (exMsg != null) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, exMsg);
    }
  }
}
