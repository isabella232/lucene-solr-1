
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import junit.framework.Assert;
import junit.framework.TestCase;

public class ProxyUserFilterTest extends SolrTestCaseJ4 {

  public void testWrongConfigGroups() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    try {
      filter.init(null);
      fail();
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail();
    }
    finally {
      filter.destroy();
    }
  }

  public void testWrongHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "otherhost");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    try {
      filter.init(null);
      fail();
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail();
    }
    finally {
      filter.destroy();
    }
  }

  public void testWrongConfigHosts() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    try {
      filter.init(null);
      fail();
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail();
    }
    finally {
      filter.destroy();
    }
  }

  public void testValidateAnyHostAnyUser() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "*");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "localhost", "bar");
    }
    finally {
      filter.destroy();
    }
  }

  public void testInvalidProxyUser() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "*");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("bar", "localhost", "foo");
      fail();
    }
    catch (AccessControlException ex) {
    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testValidateHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "localhost");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "localhost", "bar");
    }
    finally {
      filter.destroy();
    }
  }

  private String getGroup() throws Exception {
    org.apache.hadoop.security.Groups hGroups =
      new org.apache.hadoop.security.Groups(new Configuration());
    List<String> g = hGroups.getGroups(System.getProperty("user.name"));
    return g.get(0);
  }


  public void testValidateGroup() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "*");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", getGroup());
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "localhost", System.getProperty("user.name"));
    }
    finally {
      filter.destroy();
    }
  }

  public void testUnknownHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "localhost");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "unknownhost.bar.foo", "bar");
      fail();
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testInvalidHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "localhost");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "www.example.com", "bar");
      fail();
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testInvalidGroup() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "localhost");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "nobody");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "localhost", System.getProperty("user.name"));
      fail();
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testNullProxyUser() throws Exception {
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate(null, "localhost", "bar");
      fail();
    }
    catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "#USER#.hosts"));
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "#USER#.groups"));
    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testNullHost() throws Exception {
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", null, "bar");
      fail();
    }
    catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "foo.hosts"));
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "foo.groups"));
    }
    catch (Exception ex) {
      fail(ex.toString());
    }
    finally {
      filter.destroy();
    }
  }
}
