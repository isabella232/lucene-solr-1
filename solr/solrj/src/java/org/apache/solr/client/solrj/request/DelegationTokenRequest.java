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

package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

/**
 * Class for making Solr delegation token requests
 */
public abstract class DelegationTokenRequest extends SolrRequest
{
  protected static final String OP_KEY = "op";
  protected static final String TOKEN_KEY = "token";

  public DelegationTokenRequest(METHOD m) {
    // path doesn't really matter -- the filter will respond to any path.
    // setting the path to admin/collections lets us pass through CloudSolrServer
    // without having to specify a collection (that may not even exist yet).
    super(m, "/admin/collections");
  }

  protected void process(SolrServer server, DelegationTokenResponse res)
  throws SolrServerException, IOException {
    long startTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    res.setResponse( server.request( this ) );
    long endTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    res.setElapsedTime(endTime - startTime);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return null;
  }

  public static class Get extends DelegationTokenRequest {
    protected String renewer;

    public Get() {
      this(null);
    }

    public Get(String renewer) {
      super(METHOD.GET);
      this.renewer = renewer;
      setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "GETDELEGATIONTOKEN");
      if (renewer != null) params.set("renewer", renewer);
      return params;
    }

    @Override
    public DelegationTokenResponse.Get process(SolrServer server)
    throws SolrServerException, IOException {
      DelegationTokenResponse.Get res = new DelegationTokenResponse.Get();
      process(server, res);
      return res;
    }
  }

  public static class Renew extends DelegationTokenRequest {
    protected String token;

    public Renew(String token) {
      super(METHOD.PUT);
      this.token = token;
      setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
      Set<String> queryParams = new TreeSet<String>();
      queryParams.add(OP_KEY);
      queryParams.add(TOKEN_KEY);
      setQueryParams(queryParams);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "RENEWDELEGATIONTOKEN");
      params.set(TOKEN_KEY, token);
      return params;
    }

    @Override
    public DelegationTokenResponse.Renew process(SolrServer server)
    throws SolrServerException, IOException {
      DelegationTokenResponse.Renew res = new DelegationTokenResponse.Renew();
      process(server, res);
      return res;
    }
  }

  public static class Cancel extends DelegationTokenRequest {
    protected String token;

    public Cancel(String token) {
      super(METHOD.PUT);
      this.token = token;
      setResponseParser(new DelegationTokenResponse.NullResponseParser());
      Set<String> queryParams = new TreeSet<String>();
      queryParams.add(OP_KEY);
      queryParams.add(TOKEN_KEY);
      setQueryParams(queryParams);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "CANCELDELEGATIONTOKEN");
      params.set(TOKEN_KEY, token);
      return params;
    }

    @Override
    public DelegationTokenResponse.Cancel process(SolrServer server)
    throws SolrServerException, IOException {
      DelegationTokenResponse.Cancel res = new DelegationTokenResponse.Cancel();
      process(server, res);
      return res;
    }
  }
}
