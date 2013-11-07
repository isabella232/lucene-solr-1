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

import java.io.File;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.FileUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for Sentry tests
 */
public abstract class SentryTestBase extends SolrTestCaseJ4 {

  private static File sentrySite;
  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

  private static void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  @BeforeClass
  public static void setupSentry() throws Exception {
    sentrySite = File.createTempFile("sentry-site", "xml");
    File authProviderDir = new File(SolrTestCaseJ4.TEST_HOME(), "sentry");
    
    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       new File(authProviderDir.toString(), "test-authz-provider.ini").toURI().toURL().toString());
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString());

    // ensure the SentryIndexAuthorizationSingleton is created with
    // the correct sentrySite
    System.setProperty("solr.authorization.sentry.site",
      sentrySite.toURI().toURL().toString().substring("file:".length()));
    SentryIndexAuthorizationSingleton.getInstance();
    initCore("solrconfig.xml", "schema.xml");
    core = h.getCoreContainer().getCore("collection1");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
  }

  @AfterClass
  public static void teardownSentry() throws Exception {
    if (sentrySite != null) {
      FileUtils.deleteQuietly(sentrySite);
    }
    core.getCoreDescriptor().setCloudDescriptor(cloudDescriptor);
    core.close();
    sentrySite = null;
    core = null;
    cloudDescriptor = null;
  }

  protected SolrQueryRequest getRequest() {
    return new LocalSolrQueryRequest(core, new NamedList());
  }

  protected SolrQueryRequest prepareCollAndUser(SolrQueryRequest request,
      String collection, String user) {
    CloudDescriptor mCloudDescriptor = EasyMock.createMock(CloudDescriptor.class);
    EasyMock.expect(mCloudDescriptor.getCollectionName()).andReturn(collection);
    EasyMock.replay(mCloudDescriptor);
    core.getCoreDescriptor().setCloudDescriptor(mCloudDescriptor);

    HttpServletRequest httpServletRequest = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(httpServletRequest.getAttribute(SolrHadoopAuthenticationFilter.USER_NAME)).andReturn(user);
    EasyMock.replay(httpServletRequest);
    request.getContext().put("httpRequest", httpServletRequest);
    return request;
  }
}
