package org.apache.solr.core;

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
import java.io.StringReader;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.restlet.ext.servlet.ServerServlet;

/**
 * Test that a ConfigSet marked as immutable cannot be modified via
 * the known APIs, i.e. SolrConfigHandler and SchemaHandler.
 */
public class TestConfigSetImmutable extends RestTestBase {

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  public void before() throws Exception {
    File tmpSolrHome = createTempDir();
    File tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());
    // make the ConfigSet immutable
    FileUtils.write(new File(tmpConfDir, "configsetprops.json"), new StringBuilder("{\"immutable\":\"true\"}"));

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");
    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
        "/solr", true, extraServlets);
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    server = null;
    restTestHarness = null;
  }

  @Test
  public void testSchemaHandlerImmutable() throws Exception {
    final String errorMsg = "/error/msg==\"ConfigSet is immutable\"";

    // test FieldResource
    assertJPut("/schema/fields/newfield",
               json("{'type':'text','stored':false}"),
               errorMsg);

    // test FieldCollectionResource
    assertJPost("/schema/fields",
                json( "[{'name':'fieldA','type':'text','stored':false},"
                    + " {'name':'fieldB','type':'text','stored':false},"
                    + " {'name':'fieldC','type':'text','stored':false},"
                    + " {'name':'fieldD','type':'text','stored':false},"
                    + " {'name':'fieldE','type':'text','stored':false}]"),
                errorMsg);

    // test CopyFieldCollectionResource
    assertJPost("/schema/copyfields",
                json( "[{'source':'fieldA', 'dest':'fieldB'},"
                    + " {'source':'fieldD', 'dest':['fieldC', 'fieldE']}]"),
                errorMsg);
  }
}
