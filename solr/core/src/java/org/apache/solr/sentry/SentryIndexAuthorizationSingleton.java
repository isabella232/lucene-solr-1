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
import org.apache.solr.request.LocalSolrQueryRequest;
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

  public static final String propertyName = "solr.authorization.sentry.site";

  private static final SentryIndexAuthorizationSingleton INSTANCE =
    new SentryIndexAuthorizationSingleton(System.getProperty(propertyName));

  private final SolrAuthzBinding binding;

  private SentryIndexAuthorizationSingleton(String sentrySiteLocation) {
    SolrAuthzBinding tmpBinding = null;
    try {
      if (sentrySiteLocation != null && sentrySiteLocation.length() > 0) {
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

  /**
   * Attempt to authorize an administrative action.
   *
   * @param req request to check
   * @param actions set of actions to check
   * @param checkCollection check the collection the action is on, or only "admin"?
   * @param collection only relevant if checkCollection==true,
   *   use collection (if non-null) instead pulling collection name from req (if null)
   */
  public void authorizeAdminAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, boolean checkCollection, String collection)
      throws SolrException {
    authorizeCollectionAction(req, actions, "admin", true);
    if (checkCollection) {
      // Let's not error out if we can't find the collection associated with an
      // admin action, it's pretty complicated to get all the possible administrative
      // actions correct.  Instead, let's warn in the log and address any issues we
      // find.
      authorizeCollectionAction(req, actions, collection, false);
    }
  }

  /**
   * Attempt to authorize a collection action.  The collection
   * name will be pulled from the request.
   */
  public void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions) throws SolrException {
    authorizeCollectionAction(req, actions, null, true);
  }

  private void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, String collectionName, boolean errorIfNoCollection) {
    String exMsg = null;

    if (binding == null) {
      exMsg = "Solr binding was not created successfully.  Defaulting to no access";
    }
    else {
      SolrCore solrCore = req.getCore();
      HttpServletRequest httpServletRequest = (HttpServletRequest)req.getContext().get("httpRequest");
      // LocalSolrQueryRequests won't have the HttpServletRequest because there is no
      // http request associated with it.
      if (httpServletRequest == null && !(req instanceof LocalSolrQueryRequest)) {
        StringBuilder builder = new StringBuilder("Unable to locate HttpServletRequest");
        if (solrCore.getSolrConfig().getBool(
          "requestDispatcher/requestParsers/@addHttpRequestToContext", true) == false) {
          builder.append(", ensure requestDispatcher/requestParsers/@addHttpRequestToContext is set to true");
        }
        exMsg = builder.toString();
      }
      else {
        Subject superUser = new Subject(System.getProperty("solr.authorization.superuser", "solr"));
        // If a local request, treat it like a super user request; i.e. it is equivalent to an
        // http request from the same process.
        String userName = req instanceof LocalSolrQueryRequest?
          superUser.getName():(String)httpServletRequest.getAttribute(SolrHadoopAuthenticationFilter.USER_NAME);
        Subject subject = new Subject(userName);
        if (collectionName == null) {
          if (solrCore == null) {
            String msg = "Unable to locate collection for sentry to authorize because "
              + "no SolrCore attached to request";
            if (errorIfNoCollection) {
              throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, msg);
            } else { // just warn
              log.warn(msg);
              return;
            }
          }
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
