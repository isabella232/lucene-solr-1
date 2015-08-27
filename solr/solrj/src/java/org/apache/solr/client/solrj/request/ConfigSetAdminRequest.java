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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 5.4
 */
public abstract class ConfigSetAdminRequest extends SolrRequest {

  protected ConfigSetAction action = null;
  protected String configSetName = null;

  protected ConfigSetAdminRequest setAction(ConfigSetAction action) {
    this.action = action;
    return this;
  }

  public ConfigSetAdminRequest() {
    super(METHOD.GET, "/admin/configs");
  }

  public ConfigSetAdminRequest(String path) {
    super (METHOD.GET, path);
  }

  @Override
  public SolrParams getParams() {
    if (action == null) {
      throw new RuntimeException( "no action specified!" );
    }
    if (configSetName == null) {
      throw new RuntimeException( "no ConfigSet specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ConfigSetParams.ACTION, action.toString());
    params.set(NAME, configSetName);
    return params;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return null;
  }

  @Override
  public ConfigSetAdminResponse process(SolrServer server) throws SolrServerException, IOException
  {
    long startTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    ConfigSetAdminResponse res = new ConfigSetAdminResponse();
    res.setResponse( server.request( this ) );
    long endTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    res.setElapsedTime(endTime - startTime);
    return res;
  }

  public void setConfigSetName(String configSetName) {
    this.configSetName = configSetName;
  }

  public final String getConfigSetName() {
    return configSetName;
  }

  // CREATE request
  public static class Create extends ConfigSetAdminRequest {
    protected static String PROPERTY_PREFIX = "configSetProp";
    protected String baseConfigSetName;
    protected Properties properties;

    public Create() {
      action = ConfigSetAction.CREATE;
    }

    public void setBaseConfigSetName(String baseConfigSetName) {
      this.baseConfigSetName = baseConfigSetName;
    }

    public final String getBaseConfigSetName() {
      return baseConfigSetName;
    }

    public void setNewConfigSetProperties(Properties properties) {
      this.properties = properties;
    }

    public final Properties getNewConfigSetProperties() {
      return properties;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (baseConfigSetName == null) {
        throw new RuntimeException( "no Base ConfigSet specified!" );
      }
      params.set("baseConfigSet", baseConfigSetName);
      if (properties != null) {
        for (Map.Entry entry : properties.entrySet()) {
          params.set(PROPERTY_PREFIX + "." + entry.getKey().toString(),
              entry.getValue().toString());
        }
      }
      return params;
    }
  }

  // DELETE request
  public static class Delete extends ConfigSetAdminRequest {
    public Delete() {
      action = ConfigSetAction.DELETE;
    }

  }
}
