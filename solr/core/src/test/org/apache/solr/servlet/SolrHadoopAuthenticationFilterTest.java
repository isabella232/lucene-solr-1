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

import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrHadoopAuthenticationFilterTest extends SolrTestCaseJ4 {
  private static SolrHadoopAuthenticationFilter filter;

  @BeforeClass
  public static void beforeClass() throws Exception {
    filter = new SolrHadoopAuthenticationFilter();
  }

  @Test
  public void testDefaults() throws Exception {
    Properties props = filter.getConfiguration(null, null);
    assertEquals(props.getProperty(SolrHadoopAuthenticationFilter.AUTH_TYPE), PseudoAuthenticationHandler.TYPE);
    assertEquals("true", props.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }

  @Test
  public void testKerberos() throws Exception {
    String kerberos = "kerberos";
    String authType = filter.SOLR_PREFIX + filter.AUTH_TYPE;
    System.setProperty(authType, kerberos);
    Properties props = filter.getConfiguration(null, null);
    assertEquals(kerberos, props.getProperty(SolrHadoopAuthenticationFilter.AUTH_TYPE));
  }
}
