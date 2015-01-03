package org.apache.solr.update.processor;
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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for UpdateIndexAuthorizationProcessor
 */
public class UpdateIndexAuthorizationProcessorTest extends SentryTestBase {

  private UpdateIndexAuthorizationProcessorFactory factory =
    new UpdateIndexAuthorizationProcessorFactory();

  private List<String> methodNames = Arrays.asList("processAdd", "processDelete",
    "processMergeIndexes","processCommit", "processRollback", "finish");

  private void verifyAuthorized(String collection, String user) throws Exception {
    getProcessor(collection, user).processAdd(null);
    getProcessor(collection, user).processDelete(null);
    getProcessor(collection, user).processMergeIndexes(null);
    getProcessor(collection, user).processCommit(null);
    getProcessor(collection, user).processRollback(null);
    getProcessor(collection, user).finish();
  }

  private void verifyUnauthorizedException(SolrException ex, String exMsgContains, MutableInt numExceptions) {
    assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
    assertTrue(ex.getMessage().contains(exMsgContains));
    numExceptions.add(1);
  }

  private void verifyUnauthorized(String collection, String user) throws Exception {
    MutableInt numExceptions = new MutableInt(0);
    String contains = "User " + user + " does not have privileges for " + collection;

    try {
      getProcessor(collection, user).processAdd(null);
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processDelete(null);
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processMergeIndexes(null);
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processCommit(null);
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processRollback(null);
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).finish();
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }

    assertEquals(methodNames.size(), numExceptions.intValue());
  }

  private UpdateIndexAuthorizationProcessor getProcessor(String collection, String user) {
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(request, collection, user);
    return factory.getInstance(request, null, null);
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has ALL access
  */
  @Test
  public void testUpdateComponentAccessAll() throws Exception {
    verifyAuthorized("collection1", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has UPDATE only access
  */
  @Test
  public void testUpdateComponentAccessUpdate() throws Exception {
    verifyAuthorized("updateCollection", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has QUERY only access
  */
  @Test
  public void testUpdateComponentAccessQuery() throws Exception {
    verifyUnauthorized("queryCollection", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has no access
  */
  @Test
  public void testUpdateComponentAccessNone() throws Exception {
    verifyUnauthorized("noAccessCollection", "junit");
  }

  /**
   * Ensure no new methods have been added to base class that are not invoking
   * Sentry
   */
  @Test
  public void testAllMethodsChecked() throws Exception {
    Method [] methods = UpdateRequestProcessor.class.getDeclaredMethods();
    TreeSet<String> foundNames = new TreeSet<String>();
    for (Method method : methods) {
      if (Modifier.isPublic(method.getModifiers())) {
        foundNames.add(method.getName());
      }
    }
    assertEquals(methodNames.size(), foundNames.size());
    assertTrue(foundNames.containsAll(methodNames));
  }
}
