package org.apache.solr.handler.component;
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
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for QueryIndexAuthorizationComponent
 */
@org.junit.Ignore
public class QueryIndexAuthorizationComponentTest extends SentryTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    setupSentry();
    createCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore();
    teardownSentry();
  }

  private void doExpectUnauthorized(SearchComponent component,
      ResponseBuilder rb, String msgContains) throws Exception {
    try {
      component.prepare(rb);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(msgContains));
    }
  }

  private void doExpectComponentUnauthorized(SearchComponent component,
      String collection, String user) throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder.req, collection, user);
    doExpectUnauthorized(component, responseBuilder,
      "User " + user + " does not have privileges for " + collection);
  }

  private ResponseBuilder getResponseBuilder() {
    SolrQueryRequest request = getRequest();
    return new ResponseBuilder(request, null, null);
  }

  /**
   * Test the QueryIndexAuthorizationComponent on a collection that
   * the user has ALL access
   */
  @Test
  public void testQueryComponentAccessAll() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder.req, "collection1", "junit");
    QueryIndexAuthorizationComponent query = new QueryIndexAuthorizationComponent();
    query.prepare(responseBuilder);
  }

  /**
   * Test the QueryIndexAuthorizationComponent on a collection that
   * the user has QUERY only access
   */
  @Test
  public void testQueryComponentAccessQuery() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder.req, "queryCollection", "junit");
    QueryIndexAuthorizationComponent query = new QueryIndexAuthorizationComponent();
    query.prepare(responseBuilder);
  }

  /**
   * Test the QueryIndexAuthorizationComponent on a collection that
   * the user has UPDATE only access
   */
  @Test
  public void testQueryComponentAccessUpdate() throws Exception {
    doExpectComponentUnauthorized(new QueryIndexAuthorizationComponent(),
      "updateCollection", "junit");
  }

  /**
   * Test the QueryIndexAuthorizationComponent on a collection that
   * the user has no access
   */
  @Test
  public void testQueryComponentAccessNone() throws Exception {
    doExpectComponentUnauthorized(new QueryIndexAuthorizationComponent(),
      "noAccessCollection", "junit");
  }
}
