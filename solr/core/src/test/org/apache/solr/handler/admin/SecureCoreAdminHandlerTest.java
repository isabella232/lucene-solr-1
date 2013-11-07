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

import java.util.Arrays;
import java.util.List;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureCoreAdminHandlerTest extends SentryTestBase {

  public final static List<CoreAdminAction> QUERY_ACTIONS = Arrays.asList(CoreAdminAction.STATUS);
  public final static List<CoreAdminAction> UPDATE_ACTIONS = Arrays.asList(
    CoreAdminAction.LOAD,
    CoreAdminAction.UNLOAD,
    CoreAdminAction.CREATE,
    CoreAdminAction.PERSIST,
    CoreAdminAction.SWAP,
    CoreAdminAction.RENAME,
    CoreAdminAction.MERGEINDEXES,
    CoreAdminAction.SPLIT,
    CoreAdminAction.PREPRECOVERY,
    CoreAdminAction.REQUESTRECOVERY,
    CoreAdminAction.REQUESTSYNCSHARD,
    CoreAdminAction.CREATEALIAS,
    CoreAdminAction.DELETEALIAS,
    CoreAdminAction.REQUESTAPPLYUPDATES,
    CoreAdminAction.LOAD_ON_STARTUP,
    CoreAdminAction.TRANSIENT,
    // RELOAD needs to go last, because our bogus calls leaves things in a bad state for later calls.
    // We could handle this more cleanly at the cost of a lot more creating and deleting cores.
    CoreAdminAction.RELOAD
  );

  @BeforeClass
  public static void beforeClass() throws Exception {
    setupSentry();
    createCore("solrconfig-secureadmin.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore();
    teardownSentry();
  }

  private SolrQueryRequest getCoreAdminRequest(String collection, String user,
      CoreAdminAction action) {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(req, collection, user, false);
    ModifiableSolrParams modParams = new ModifiableSolrParams(req.getParams());
    modParams.set(CoreAdminParams.ACTION, action.name());
    req.setParams(modParams);
    return req;
  }

  private void verifyQueryAccess(CoreAdminAction action) throws Exception {
    CoreAdminHandler handler = h.getCoreContainer().getMultiCoreHandler();
    verifyAuthorized(handler, getCoreAdminRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCoreAdminRequest("queryCollection", "junit", action));
    verifyUnauthorized(handler, getCoreAdminRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
    verifyUnauthorized(handler, getCoreAdminRequest("updateCollection", "junit", action), "updateCollection", "junit");
  }

  private void verifyUpdateAccess(CoreAdminAction action) throws Exception {
    CoreAdminHandler handler = h.getCoreContainer().getMultiCoreHandler();
    verifyAuthorized(handler, getCoreAdminRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCoreAdminRequest("updateCollection", "junit", action));
    verifyUnauthorized(handler, getCoreAdminRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
    verifyUnauthorized(handler, getCoreAdminRequest("queryCollection", "junit", action), "queryCollection", "junit");
  }

  @Test
  public void testSecureAdminHandler() throws Exception {
    for (CoreAdminAction action : QUERY_ACTIONS) {
      verifyQueryAccess(action);
    }
    for (CoreAdminAction action : UPDATE_ACTIONS) {
      verifyUpdateAccess(action);
    }
  }

  @Test
  public void testAllActionsChecked() throws Exception {
    for (CoreAdminAction action : CoreAdminAction.values()) {
      assertTrue(QUERY_ACTIONS.contains(action) || UPDATE_ACTIONS.contains(action));
    }
  }
}
