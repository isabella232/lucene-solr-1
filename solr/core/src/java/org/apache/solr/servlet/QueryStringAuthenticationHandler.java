package org.apache.solr.servlet;
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import  org.apache.hadoop.security.authentication.server.AuthenticationToken;
import  org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.NameValuePair;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * PseudoAuthenticationHandler that parses the query string rather than
 * calling getParameter on the HttpServletRequest, to avoid affecting
 * later calls to HttpServletRequest getInputStream.
 */
public class QueryStringAuthenticationHandler extends PseudoAuthenticationHandler {

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = QueryStringAuthenticationHandler.class.getCanonicalName();

  private String getUserName(HttpServletRequest request) {
    List<NameValuePair> list = URLEncodedUtils.parse(request.getQueryString(),Charset.forName("UTF-8"));
    if (list != null) {
      for (NameValuePair nv : list) {
        if (PseudoAuthenticator.USER_NAME.equals(nv.getName())) {
          return nv.getValue();
        }
      }
    }
    return null;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
    throws IOException, AuthenticationException {
    AuthenticationToken token;
    String userName = getUserName(request);
    if (userName == null) {
      if (getAcceptAnonymous()) {
        token = AuthenticationToken.ANONYMOUS;
      } else {
        throw new AuthenticationException("Anonymous requests are disallowed");
      }
    } else {
      token = new AuthenticationToken(userName, userName, getType());
    }
    return token;
  }
}
