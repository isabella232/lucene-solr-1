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

package org.apache.solr.servlet;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.solr.SolrTestCaseJ4;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryStringAuthenticationHandlerTest extends SolrTestCaseJ4 {
  private static QueryStringAuthenticationHandler handler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    handler = new QueryStringAuthenticationHandler();
  }

  private boolean verifyAuthenticationToken(String expectedUser, AuthenticationToken token) {
    return expectedUser.equals(token.getUserName()) && expectedUser.equals(token.getName());
  }

  private boolean verifyHandler(String url, String expectedUser) throws Exception {
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getQueryString()).andReturn(url);
    EasyMock.replay(request);
    AuthenticationToken token = handler.authenticate(request, null);
    return verifyAuthenticationToken(expectedUser, token);
  }

  @Test
  public void testNull() throws Exception {
    try {
      assertTrue(verifyHandler("", null));
      Assert.fail("Expected AuthenticationException");
    } catch (AuthenticationException ex) {
      // expected
    }
  }

  @Test
  public void testUserFirst() throws Exception {
    assertTrue(verifyHandler("user.name=hue&foobar=foobar&doAs=solr", "hue"));
  }

  @Test
  public void testUserMiddle() throws Exception {
    assertTrue(verifyHandler("doAs=solr&user.name=solr&foobar=foobar", "solr"));
  }

  @Test
  public void testUserLast() throws Exception {
    assertTrue(verifyHandler("foobar=foobar&doAs=solr&user.name=junit", "junit"));
  }
}
