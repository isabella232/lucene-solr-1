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

import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecureAdminHandlersTest extends SentryTestBase {

  private static Logger log = LoggerFactory.getLogger(LukeRequestHandler.class);

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

  @Test
  public void testAllAdminHandlersSecured() throws Exception {
    int numFound = 0;
    for (Map.Entry<String, SolrRequestHandler> entry : getCore().getRequestHandlers().entrySet() ) {
      if (entry.getKey().startsWith("/admin/")) {
         assertTrue(entry.getValue() instanceof SecureAdminHandlerWrapper);
         ++numFound;
      }
    }
    assertTrue(numFound > 0);
  }

  @Test
  public void testSecureAdminHandlers() throws Exception {
    verifyLuke();
    verifyMBeans();
    verifyPlugins();
    verifyThreads();
    verifyProperties();
    verifyLogging();
    verifyFile();
  }

  private void verifyAuthorized(SecureAdminHandlerWrapper handler, String collection, String user) throws Exception {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(req, collection, user, false);
    // just ensure we don't get an unauthorized exception
    try {
      handler.handleRequestBody(req, new SolrQueryResponse());
    } catch (SolrException ex) {
      assertFalse(ex.code() == SolrException.ErrorCode.UNAUTHORIZED.code);
    } catch (Throwable t) {
      // okay, we only want to verify we didn't get an Unauthorized exception,
      // going to treat each handler as a block box.
    }
  }

  private void verifyUnauthorized(SecureAdminHandlerWrapper handler,
      String collection, String user) throws Exception {
    String exMsgContains = "User " + user + " does not have privileges for " + collection;
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(req, collection, user, false);
    try {
      handler.handleRequestBody(req, new SolrQueryResponse());
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(exMsgContains));
    }
  }

  private void verifyQueryAccess(String path) throws Exception {
    SecureAdminHandlerWrapper handler =
      (SecureAdminHandlerWrapper)getCore().getRequestHandlers().get(path);
    verifyAuthorized(handler, "collection1", "junit");
    verifyAuthorized(handler, "queryCollection", "junit");
    verifyUnauthorized(handler, "bogsuCollection", "junit");
    verifyUnauthorized(handler, "updateCollection", "junit");
  }

  private void verifyQueryUpdateAccess(String path) throws Exception {
    SecureAdminHandlerWrapper handler =
      (SecureAdminHandlerWrapper)getCore().getRequestHandlers().get(path);
    verifyAuthorized(handler, "collection1", "junit");
    verifyUnauthorized(handler, "queryCollection", "junit");
    verifyUnauthorized(handler, "bogusCollection", "junit");
    verifyUnauthorized(handler, "updateCollection", "junit");
  }

  private void verifyLuke() throws Exception {
    verifyQueryAccess("/admin/luke");
  }

  private void verifySystem() throws Exception {
    verifyQueryAccess("/admin/system");
  }

  private void verifyMBeans() throws Exception {
    verifyQueryAccess("/admin/mbeans");
  }

  private void verifyPlugins() throws Exception {
    verifyQueryAccess("/admin/plugins");
  }

  private void verifyThreads() throws Exception {
    verifyQueryAccess("/admin/threads");
  }

  private void verifyProperties() throws Exception {
    verifyQueryAccess("/admin/properties");
  }

  private void verifyLogging() throws Exception {
    verifyQueryUpdateAccess("/admin/logging");
  }

  private void verifyFile() throws Exception {
    verifyQueryAccess("/admin/file");
  }
}
