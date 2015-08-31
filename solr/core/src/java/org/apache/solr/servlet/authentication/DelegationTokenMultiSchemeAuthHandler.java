/**
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
package org.apache.solr.servlet.authentication;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import com.google.common.base.Preconditions;


/**
 * An {@link AuthenticationHandler} that supports multiple HTTP authentication schemes
 * along with Delegation Token functionality.
 *
 * <p/>
 * In addition to the wrapped {@link AuthenticationHandler} configuration
 * properties, this handler supports the following properties prefixed
 * with the type of the wrapped <code>AuthenticationHandler</code>:
 * <ul>
 * <li>delegation-token.token-kind: the token kind for generated tokens
 * (no default, required property).</li>
 * <li>delegation-token.update-interval.sec: secret manager master key
 * update interval in seconds (default 1 day).</li>
 * <li>delegation-token.max-lifetime.sec: maximum life of a delegation
 * token in seconds (default 7 days).</li>
 * <li>delegation-token.renewal-interval.sec: renewal interval for
 * delegation tokens in seconds (default 1 day).</li>
 * <li>delegation-token.removal-scan-interval.sec: delegation tokens
 * removal scan interval in seconds (default 1 hour).</li>
 * </ul>
 * <li>multi-scheme-auth-handler.delegation.http.schemes: A comma separated
 * list of HTTP authentication mechanisms (e.g. Negotiate, Basic) etc. to
 * be allowed for delegation token management operations.
 */
public class DelegationTokenMultiSchemeAuthHandler extends
    DelegationTokenAuthenticationHandler {

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  public static final String DELEGATION_TOKEN_SCHEMES_PROPERTY = "multi-scheme-auth-handler.delegation.schemes";

  /**
   * This code is duplicated from Hadoop DelegationTokenAuthenticationHandler class
   * to support configurable authentication mechanism for delegation tokens. Please
   * check the {@linkplain #authenticate(HttpServletRequest, HttpServletResponse)}
   * method for more details.
   */
  private static final Set<String> DELEGATION_TOKEN_OPS = new HashSet<String>();

  static {
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.GETDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    DELEGATION_TOKEN_OPS.add(KerberosDelegationTokenAuthenticator.
        DelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
  }

  private Set<String> delegationAuthSchemes = null;

  public DelegationTokenMultiSchemeAuthHandler() {
    super(new MultiSchemeAuthenticationHandler(MultiSchemeAuthenticationHandler.TYPE + TYPE_POSTFIX));
  }

  @Override
  public void init(Properties config) throws ServletException {
    super.init(config);

    //Figure out the HTTP authentication schemes configured.
    String schemesProperty = config.getProperty(MultiSchemeAuthenticationHandler.SCHEMES_PROPERTY);
    Preconditions.checkNotNull(schemesProperty);

    //Figure out the HTTP authentication schemes configured for delegation tokens.
    String delegationAuthSchemesProp = config.getProperty(DELEGATION_TOKEN_SCHEMES_PROPERTY);
    Preconditions.checkNotNull(delegationAuthSchemesProp);

    Set<String> authSchemes = new HashSet<>(Arrays.asList(schemesProperty.split(",")));
    delegationAuthSchemes = new HashSet<>(Arrays.asList(delegationAuthSchemesProp.split(",")));

    Preconditions.checkArgument(authSchemes.containsAll(delegationAuthSchemes));
  }

  /**
   * This method is overridden to restrict HTTP authentication schemes available for
   * delegation token management functionality. The authentication schemes to be used for
   * delegation token management are configured using {@linkplain
   *  DelegationTokenMultiSchemeAuthHandler#DELEGATION_TOKEN_SCHEMES_PROPERTY}
   *
   * The basic logic here is to check if the current request is for delegation token management.
   * If yes then check if the request contains an "Authorization" header. If it is missing, then
   * return the HTTP 401 response with WWW-Authenticate header for each scheme configured for
   * delegation token management.
   *
   * It is also possible for a client to preemptively send Authorization header for a scheme not
   * configured for delegation token management. We detect this case and return the HTTP 401 response
   * with WWW-Authenticate header for each scheme configured for delegation token management.
   *
   * If a client has sent a request with "Authorization" header for a scheme configured for
   * delegation token management, then it is forwarded to underlying {@link MultiSchemeAuthenticationHandler}
   * for actual authentication.
   *
   * Finally all other requests (excluding delegation token management) are forwarded to  underlying
   * {@link MultiSchemeAuthenticationHandler} for actual authentication.
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request,
      HttpServletResponse response) throws IOException, AuthenticationException {
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    String op = getParameter(request, KerberosDelegationTokenAuthenticator.OP_PARAM);
    op = (op != null) ? toUpperCase(op) : null;

    if(DELEGATION_TOKEN_OPS.contains(op) && !request.getMethod().equals("OPTIONS")) {
      boolean schemeConfigured = false;
      if(authorization != null) {
        for ( String scheme : delegationAuthSchemes ) {
          if(authorization.startsWith(scheme)) {
            schemeConfigured = true;
            break;
          }
        }
      }
      if(!schemeConfigured) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        for (String scheme : delegationAuthSchemes) {
          response.addHeader(WWW_AUTHENTICATE, scheme);
        }
        return null;
      }
    }

    return super.authenticate(request, response);
  }

  /**
   * Extract a query string parameter without triggering http parameters
   * processing by the servlet container.
   *
   * @param request the request
   * @param name the parameter to get the value.
   * @return the parameter value, or <code>NULL</code> if the parameter is not
   * defined.
   * @throws IOException thrown if there was an error parsing the query string.
   */
  public static String getParameter(HttpServletRequest request, String name)
      throws IOException {
    List<NameValuePair> list = URLEncodedUtils.parse(request.getQueryString(),
        UTF8_CHARSET);
    if (list != null) {
      for (NameValuePair nv : list) {
        if (name.equals(nv.getName())) {
          return nv.getValue();
        }
      }
    }
    return null;
  }

  /**
   * Converts all of the characters in this String to upper case with
   * Locale.ENGLISH.
   *
   * @param str  string to be converted
   * @return     the str, converted to uppercase.
   */
  public static String toUpperCase(String str) {
    return str.toUpperCase(Locale.ENGLISH);
  }

}
