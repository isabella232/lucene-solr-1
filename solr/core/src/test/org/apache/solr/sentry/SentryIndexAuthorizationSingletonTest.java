package org.apache.solr.sentry;
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

import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;
import org.apache.solr.request.SolrQueryRequest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for SentryIndexAuthorizationSingleton
 */
@Ignore
public class SentryIndexAuthorizationSingletonTest extends SentryTestBase {

  /**
   * Expect an unauthorized SolrException with a message that contains
   * msgContains.
   */
  private void doExpectUnauthorized(SolrQueryRequest request,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    doExpectUnauthorized(SentryIndexAuthorizationSingleton.getInstance(), request, actions, msgContains);
  }

  private void doExpectUnauthorized(SentryIndexAuthorizationSingleton singleton, SolrQueryRequest request,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    try {
      singleton.authorizeCollectionAction(request, actions);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(msgContains));
    }
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
    SolrQueryRequest request = getRequest();
    doExpectUnauthorized(request, null, "HttpServletRequest");
  }

 @Test
  public void testNullUserName() throws Exception {
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(request, "collection1", null);
    doExpectUnauthorized(request, EnumSet.of(SearchModelAction.ALL),
      "User null does not have privileges for collection1");
  }

  @Test
  public void testEmptySuperUser() throws Exception {
    System.setProperty("solr.authorization.superuser", "");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(request, "collection1", "solr");
    doExpectUnauthorized(request, EnumSet.of(SearchModelAction.ALL),
      "User solr does not have privileges for collection1");
  }

  /**
   * User name matches super user, should have access otherwise
   */
  @Test
  public void testSuperUserAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(request, "collection1", "junit");

    SentryIndexAuthorizationSingleton.getInstance().authorizeCollectionAction(
      request, EnumSet.of(SearchModelAction.ALL));
  }

  /**
   * User name matches super user, should not have access otherwise
   */
  @Test
  public void testSuperUserNoAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(request, "bogusCollection", "junit");

    SentryIndexAuthorizationSingleton.getInstance().authorizeCollectionAction(
      request, EnumSet.of(SearchModelAction.ALL));
  }
}
