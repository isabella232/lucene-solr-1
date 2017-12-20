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
package org.apache.solr.security;

import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

import org.apache.solr.common.params.SolrParams;

/**
 * Request context for Solr to be used by Authorization plugin.
 */
public abstract class AuthorizationContext {

  public static class CollectionRequest {
    final public String collectionName;

    public CollectionRequest(String collectionName) {
      this.collectionName = collectionName;
    }
  }
  
  public abstract SolrParams getParams() ;

  /**
   * This method returns the {@linkplain Principal} corresponding to
   * the authenticated user for the current request. Please note that
   * the value returned by {@linkplain Principal#getName()} method depends
   * upon the type of the authentication mechanism used (e.g. for user "foo"
   * with BASIC authentication the result would be "foo". On the other hand
   * with KERBEROS it would be foo@RELMNAME). Hence
   * {@linkplain AuthorizationContext#getUserName()} method should be preferred
   * to extract the identity of authenticated user instead of this method.
   *
   * @return user principal in case of an authenticated request
   *         null in case of unauthenticated request
   */
  public abstract Principal getUserPrincipal() ;

  /**
   * This method returns the name of the authenticated user for the current request.
   * The return value of this method is agnostic of the underlying authentication
   * mechanism used.
   *
   * @return user name in case of an authenticated user
   *         null in case of unauthenticated request
   */
  public abstract String getUserName() ;

  /**
   * This method returns the name of the user impersonating on behalf of some other user
   * in the system. e.g. in case of web application proxying requests to the Solr service,
   * the impersonator would be the user used by the web application to authenticate against
   * the Solr service. On the other hand, {@link #getUserName()} will return the name of the
   * user who initiated the original request.
   *
   * @return impersonator username in case impersonation is used
   *         none if impersonation is not used (e.g. user directly queried Solr service).
   */
  public abstract Optional<String> getImpersonatorUserName();

  public abstract String getHttpHeader(String header);
  
  public abstract Enumeration getHeaderNames();

  public abstract String getRemoteAddr();

  public abstract String getRemoteHost();

  public abstract List<CollectionRequest> getCollectionRequests() ;
  
  public abstract RequestType getRequestType();
  
  public abstract String getResource();

  public abstract String getHttpMethod();

  public enum RequestType {READ, WRITE, ADMIN, UNKNOWN}

  public abstract Object getHandler();

}