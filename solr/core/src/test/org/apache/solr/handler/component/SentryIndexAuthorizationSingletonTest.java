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

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for SentryIndexAuthorizationSingleton
 */
public class SentryIndexAuthorizationSingletonTest extends SolrTestCaseJ4 {

  private static File sentrySite;
  private SolrCore core;

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

 @BeforeClass
  public static void setupSentry() throws Exception {
    sentrySite = File.createTempFile("sentry-site", "xml");
    File authProviderDir = new File(SolrTestCaseJ4.TEST_HOME(), "sentry");
    
    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       new File(authProviderDir.toString(), "test-authz-provider.ini").toURI().toURL().toString());
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString());

    // ensure the SentryIndexAuthorizationSingleton is created with
    // the correct sentrySite
    System.setProperty("solr.authorization.sentry.site",
      sentrySite.toURI().toURL().toString());
    SentryIndexAuthorizationSingleton.getInstance();
  }

  @AfterClass
  public static void teardownSentry() throws Exception {
    if (sentrySite != null) {
      FileUtils.deleteQuietly(sentrySite);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    initCore("solrconfig.xml", "schema.xml");
    core = h.getCoreContainer().getCore("collection1");
  }

  @Override
  public void tearDown() throws Exception {
    core.close();
    deleteCore();
    super.tearDown();
  }

  /**
   * Expect an unauthorized SolrException with a message that contains
   * msgContains.
   */
  private void doExpectUnauthorized(ResponseBuilder rb,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    doExpectUnauthorized(SentryIndexAuthorizationSingleton.getInstance(), rb, actions, msgContains);
  }

  private void doExpectUnauthorized(SentryIndexAuthorizationSingleton singleton, ResponseBuilder rb,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    try {
      singleton.authorizeCollectionAction(rb, actions);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(msgContains));
    }
  }

  private void doExpectUnauthorized(SearchComponent component, ResponseBuilder rb, String msgContains)
  throws Exception {
    try {
      component.prepare(rb);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(msgContains));
    }
  }

  private ResponseBuilder getResponseBuilder() {
    return new ResponseBuilder(new LocalSolrQueryRequest(core, new NamedList()), null, null);
  }

  private ResponseBuilder prepareCollAndUser(ResponseBuilder responseBuilder,
      String collection, String user) {
    CloudDescriptor cloudDescriptor = EasyMock.createMock(CloudDescriptor.class);
    EasyMock.expect(cloudDescriptor.getCollectionName()).andReturn(collection);
    EasyMock.replay(cloudDescriptor);
    core.getCoreDescriptor().setCloudDescriptor(cloudDescriptor);

    HttpServletRequest httpServletRequest = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(httpServletRequest.getAttribute(SolrHadoopAuthenticationFilter.USER_NAME)).andReturn(user);
    EasyMock.replay(httpServletRequest);
    responseBuilder.req.getContext().put("httpRequest", httpServletRequest);
    return responseBuilder;
  }

  private void doExpectComponentUnauthorized(SearchComponent component,
      String collection, String user) throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, collection, user);
    doExpectUnauthorized(component, responseBuilder,
      "User " + user + " does not have privileges for " + collection);
  }

  @Test
  public void testNoBinding() throws Exception {
    // Use reflection to construct a non-singleton version of SentryIndexAuthorizationSingleton
    // in order to get an instance without a binding
    System.setProperty("solr.authorization.sentry.site", "");
    Constructor ctor =
      SentryIndexAuthorizationSingleton.class.getDeclaredConstructor();
    ctor.setAccessible(true);
    SentryIndexAuthorizationSingleton nonSingleton =
      (SentryIndexAuthorizationSingleton)ctor.newInstance();
    doExpectUnauthorized(nonSingleton, null, null, "binding");
  }

  @Test
  public void testNoHttpRequest() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    doExpectUnauthorized(responseBuilder, null, "HttpServletRequest");
  }

 @Test
  public void testNullUserName() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "collection1", null);
    doExpectUnauthorized(responseBuilder, EnumSet.of(SearchModelAction.ALL),
      "User null does not have privileges for collection1");
  }

  @Test
  public void testEmptySuperUser() throws Exception {
    System.setProperty("solr.authorization.superuser", "");
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "collection1", "solr");
    doExpectUnauthorized(responseBuilder, EnumSet.of(SearchModelAction.ALL),
      "User solr does not have privileges for collection1");
  }

  /**
   * User name matches super user, should have access otherwise
   */
  @Test
  public void testSuperUserAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "collection1", "junit");

    SentryIndexAuthorizationSingleton.getInstance().authorizeCollectionAction(
      responseBuilder, EnumSet.of(SearchModelAction.ALL));
  }

  /**
   * User name matches super user, should not have access otherwise
   */
  @Test
  public void testSuperUserNoAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "bogusCollection", "junit");

    SentryIndexAuthorizationSingleton.getInstance().authorizeCollectionAction(
      responseBuilder, EnumSet.of(SearchModelAction.ALL));
  }

  /**
   * Test the QueryIndexAuthorizationComponent on a collection that
   * the user has ALL access
   */
  @Test
  public void testQueryComponentAccessAll() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "collection1", "junit");
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
    prepareCollAndUser(responseBuilder, "queryCollection", "junit");
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

  /**
   * Test the UpdateIndexAuthorizationComponent on a collection that
   * the user has ALL access
   */
  @Test
  public void testUpdateComponentAccessAll() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "collection1", "junit");
    UpdateIndexAuthorizationComponent update = new UpdateIndexAuthorizationComponent();
    update.prepare(responseBuilder);
  }

  /**
   * Test the UpdateIndexAuthorizationComponent on a collection that
   * the user has UPDATE only access
   */
  @Test
  public void testUpdateComponentAccessUpdate() throws Exception {
    ResponseBuilder responseBuilder = getResponseBuilder();
    prepareCollAndUser(responseBuilder, "updateCollection", "junit");
    UpdateIndexAuthorizationComponent update = new UpdateIndexAuthorizationComponent();
    update.prepare(responseBuilder);
  }

  /**
   * Test the UpdateIndexAuthorizationComponent on a collection that
   * the user has QUERY only access
   */
  @Test
  public void testUpdateComponentAccessQuery() throws Exception {
    doExpectComponentUnauthorized(new UpdateIndexAuthorizationComponent(),
      "queryCollection", "junit");
  }

  /**
   * Test the UpdateIndexAuthorizationComponent on a collection that
   * the user has no access
   */
  @Test
  public void testUpdateComponentAccessNone() throws Exception {
    doExpectComponentUnauthorized(new UpdateIndexAuthorizationComponent(),
      "noAccessCollection", "junit");
  }
}
