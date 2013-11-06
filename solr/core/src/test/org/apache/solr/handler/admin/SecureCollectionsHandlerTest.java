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

import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureCollectionsHandlerTest extends SentryTestBase {

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

  private SolrQueryRequest getCollectionsRequest(String collection, String user,
      CollectionAction action) {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(req, collection, user, false);
    ModifiableSolrParams modParams = new ModifiableSolrParams(req.getParams());
    modParams.set(CoreAdminParams.ACTION, action.name());
    req.setParams(modParams);
    return req;
  }

  private void verifyUpdateAccess(CollectionAction action) throws Exception {
    CollectionsHandler handler = h.getCoreContainer().getCollectionsHandler();
    verifyAuthorized(handler, getCollectionsRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCollectionsRequest("updateCollection", "junit", action));
    verifyUnauthorized(handler, getCollectionsRequest("queryCollection", "junit", action), "queryCollection", "junit");
    verifyUnauthorized(handler, getCollectionsRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
  }

  @Test
  public void testSecureCollectionsHandler() throws Exception {
    for (CollectionAction action : CollectionAction.values()) {
      verifyUpdateAccess(action);
    }
  }
}
