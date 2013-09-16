
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

import java.security.AccessControlException;
import java.util.List;

import javax.servlet.ServletException;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.SolrTestCaseJ4;

public class ProxyUserFilterTest extends SolrTestCaseJ4 {

  public void testWrongConfigGroups() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    try {
      filter.init(null);
      fail("Expected ServletException exception");
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail("Expected ServletException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testWrongHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "1.1.1.1.1.1");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    try {
      filter.init(null);
      fail("Expected ServletException exception");
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail("Expected ServletException exception : " + ex.toString());
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
      fail("Expected ServletException exception");
    }
    catch (ServletException ex) {
    }
    catch (Exception ex) {
      fail("Expected ServletException exception : " + ex.toString());
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
      filter.validate("foo", "127.0.0.1", "bar");
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
      filter.validate("bar", "127.0.0.1", "foo");
      fail("Expected AccessControlException exception");
    }
    catch (AccessControlException ex) {
    }
    catch (Exception ex) {
      fail("expected AccessControlException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testValidateHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "127.0.0.1");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "127.0.0.1", "bar");
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
      filter.validate("foo", "127.0.0.1", System.getProperty("user.name"));
    }
    finally {
      filter.destroy();
    }
  }

  public void testUnknownHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "127.0.0.1");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "unknownhost.bar.foo", "bar");
      fail("expected AccessControlException exception");
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail("expected AccessControlException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testInvalidHost() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "127.0.0.1");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "*");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "[ff01::114]", "bar");
      fail("Expected AccessControlException exception");
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail("Expected AccessControlException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testInvalidGroup() throws Exception {
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.hosts", "127.0.0.1");
    System.setProperty(ProxyUserFilter.CONF_PREFIX + "foo.groups", "nobody");
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate("foo", "127.0.0.1", System.getProperty("user.name"));
      fail("Expected AccessControlException exception");
    }
    catch (AccessControlException ex) {

    }
    catch (Exception ex) {
      fail("Expected AccessControlException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }

  public void testNullProxyUser() throws Exception {
    ProxyUserFilter filter = new ProxyUserFilter();
    filter.init(null);
    try {
      filter.validate(null, "127.0.0.1", "bar");
      fail("Expected IllegalArgumentException exception");
    }
    catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "#USER#.hosts"));
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "#USER#.groups"));
    }
    catch (Exception ex) {
      fail("Expected IllegalArgumentException exception : " + ex.toString());
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
      fail("Expected IllegalArgumentException exception");
    }
    catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "foo.hosts"));
      assertTrue(ex.getMessage().contains(ProxyUserFilter.CONF_PREFIX + "foo.groups"));
    }
    catch (Exception ex) {
      fail("Expected IllegalArgumentException exception : " + ex.toString());
    }
    finally {
      filter.destroy();
    }
  }
}
