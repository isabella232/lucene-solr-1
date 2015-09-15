/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.solr.servlet.authentication;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * The {@link MultiSchemeAuthenticationHandler} supports configuring multiple
 * authentication mechanisms simultaneously.
 * <p/>
 * The supported configuration properties are:
 * <ul>
 * <li>multi-scheme-auth-handler.schemes: A comma separated list of HTTP
 * authentication mechanisms supported by this handler. It does not have a
 * default value. e.g. multi-scheme-auth-handler.schemes=Basic,Negotiate</li>
 * <li>multi-scheme-auth-handler.schemes.<i>scheme-name</i>.handler: The
 * authentication handler implementation to be used for the specified
 * authentication scheme. It does not have a default value. e.g.
 * multi-scheme-auth-handler.schemes.negotiate.handler=kerberos
 *
 * It expected that for every authentication scheme specified in
 * multi-scheme-auth-handler.schemes property, a handler needs to be configured.
 * </li>
 * </ul>
 */
public class MultiSchemeAuthenticationHandler implements AuthenticationHandler {
  private static Logger LOG = LoggerFactory.getLogger(MultiSchemeAuthenticationHandler.class);
  public static final String SCHEMES_PROPERTY = "multi-scheme-auth-handler.schemes";
  public static final String AUTH_HANDLER_PROPERTY = "multi-scheme-auth-handler.schemes.%s.handler";
  public static final Collection<String> SUPPORTED_HTTP_AUTH_SCHEMES = Arrays.asList("Basic", "Negotiate", "Digest");

  private final Map<String, AuthenticationHandler> schemeToAuthHandlerMapping = new HashMap<>();
  private static Collection<String> types = null;
  private final String authType;

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "multi-auth-handler";

  public MultiSchemeAuthenticationHandler() {
    this(TYPE);
  }

  public MultiSchemeAuthenticationHandler(String authType) {
    this.authType = authType;
  }

  @Override
  public String getType() {
    return authType;
  }

  public static Collection<String> getTypes() {
    return types;
  }

  @Override
  public void init(Properties config) throws ServletException {
    // Useful for debugging purpose.
    for (Object propName : config.keySet()) {
      Object propValue = config.get(propName);
      LOG.debug("{} : {}", propName, propValue);
    }

    types = new HashSet<>();
    String schemesProperty = config.getProperty(SCHEMES_PROPERTY);
    String[] schemes = schemesProperty.split(",");
    for (String scheme : schemes) {
      if (!SUPPORTED_HTTP_AUTH_SCHEMES.contains(scheme)) {
        throw new IllegalArgumentException("Unsupported HTTP authentication scheme " + scheme + " . Supported schemes are "
            + SUPPORTED_HTTP_AUTH_SCHEMES);
      }
      if (schemeToAuthHandlerMapping.containsKey(scheme)) {
        throw new IllegalArgumentException("Handler is already specified for " + scheme + " authentication scheme.");
      }

      String authHandlerPropName = String.format(Locale.ENGLISH, AUTH_HANDLER_PROPERTY, scheme).toLowerCase(Locale.ENGLISH);
      String authHandlerName = config.getProperty(authHandlerPropName);
      Preconditions.checkNotNull(authHandlerName, "No auth handler configured for scheme %s.", scheme);

      String authHandlerClassName = getAuthenticationHandlerClassName(authHandlerName);
      AuthenticationHandler handler = initializeAuthHandler(authHandlerClassName, config);
      schemeToAuthHandlerMapping.put(scheme, handler);
      types.add(handler.getType());
    }
    LOG.info("Successfully initialized MultiSchemeAuthenticationHandler");
  }

  /**
   * This method provides an instance of {@link AuthenticationHandler} based on
   * specified <code>authHandlerName</code>.
   *
   * @param authHandlerName The short-name (or fully qualified class name) of
   *          the authentication handler.
   * @return an instance of AuthenticationHandler implementation.
   */
  protected String getAuthenticationHandlerClassName(String authHandlerName) {
    Preconditions.checkNotNull(authHandlerName);

    String authHandlerClassName = null;

    if (authHandlerName.toLowerCase(Locale.ENGLISH).equals(PseudoAuthenticationHandler.TYPE)) {
      authHandlerClassName = PseudoAuthenticationHandler.class.getName();
    } else if (authHandlerName.toLowerCase(Locale.ENGLISH).equals(KerberosAuthenticationHandler.TYPE)) {
      authHandlerClassName = KerberosAuthenticationHandler.class.getName();
    } else if (authHandlerName.toLowerCase(Locale.ENGLISH).equals(LdapAuthenticationHandler.TYPE)) {
      authHandlerClassName = LdapAuthenticationHandler.class.getName();
    } else {
      authHandlerClassName = authHandlerName;
    }

    return authHandlerClassName;
  }

  protected AuthenticationHandler initializeAuthHandler(String authHandlerClassName, Properties config) throws ServletException {
    try {
      Preconditions.checkNotNull(authHandlerClassName);
      LOG.debug("Initializing Authentication handler of type " + authHandlerClassName);
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(authHandlerClassName);
      AuthenticationHandler authHandler = (AuthenticationHandler) klass.newInstance();
      authHandler.init(config);
      LOG.info("Successfully initialized Authentication handler of type " + authHandlerClassName);
      return authHandler;
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      LOG.error("Failed to initialize authentication handler " + authHandlerClassName, ex);
      throw new ServletException(ex);
    }
  }

  @Override
  public void destroy() {
    for (AuthenticationHandler handler : schemeToAuthHandlerMapping.values()) {
      handler.destroy();
    }
  }

  @Override
  public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    return true;
  }

  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException,
      AuthenticationException {
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    if (authorization == null) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      for (String scheme : schemeToAuthHandlerMapping.keySet()) {
        response.addHeader(WWW_AUTHENTICATE, scheme);
      }
    } else {
      for (String scheme : schemeToAuthHandlerMapping.keySet()) {
        if (authorization.startsWith(scheme)) {
          AuthenticationHandler handler = schemeToAuthHandlerMapping.get(scheme);
          AuthenticationToken token = handler.authenticate(request, response);
          LOG.debug("Token generated with type {}", token.getType());
          return token;
        }
      }
    }
    return null;
  }

}
