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

package org.apache.solr.upgrade;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.config.upgrade.UpgradeConfigException;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import static org.apache.solr.util.RegexMatcher.matchesPattern;
import static org.hamcrest.CoreMatchers.allOf;
import static org.junit.matchers.JUnitMatchers.hasItem;

public class SolrXmlUpgradeValidatorTest extends UpgradeTestBase {
  @After
  public void dumpSolrLogFileIfTestFailed() {
    if(solr4 != null) {
      solr4.dumpLogFileIfPossible();
    }
    if(solr != null) {
      solr.dumpLogFileIfPossible();
    }
  }


  public static final String INCOMPATIBILITY_DESCRIPTIONS = "/result/incompatibility[contains(level, '%s')]/description";

  @Test
  public void verifyInvalidItemsInOldFormat() throws Exception {
    startSolrWithXml("solr-4.10.3-nonupgradeable.xml");
    Path solrXml = Paths.get(TEST_HOME(), "solr-4.10.3-nonupgradeable.xml");

    generateSolrXmlUpgradeFailures(solrXml);

    assertMatchesAll(solrXmlIncompatibilities("error"),
        matchesPattern("no.*support.*cores element")
    );
  }

  @Test
  public void verifyInvalidItemsInNewFormat() throws Exception {
    startSolrWithXml("solr-4.10.3-nonupgradeable-newformat.xml");
    Path solrXml = Paths.get(TEST_HOME(), "solr-4.10.3-nonupgradeable-newformat.xml");

    generateSolrXmlUpgradeFailures(solrXml);

    assertMatchesAll(solrXmlIncompatibilities("error"),
        matchesPattern("SecureCoreAdminHandler.*removed"),
        matchesPattern("SecureCollectionsHandler.*removed"),
        matchesPattern("SecureConfigSetsHandler.*removed"),
        matchesPattern("SecureInfoHandler.*removed")
    );
  }

  private void generateSolrXmlUpgradeFailures(Path solrXml) {
    try {
      UpgradeToolUtil.doUpgradeSolrXml(solrXml, upgradedDir, false);
      fail();
    } catch (UpgradeConfigException ignore) {
    }
  }

  private void startSolrWithXml(String solrXml) throws IOException {
    createSolr4Cluster();

    dockerRunner.copySolrXml(new File(solr4.getNodeDir()), TEST_HOME(), solrXml);
    solr4.start();
    stopSolr4();
  }

  private void assertMatchesAll(Set<String> findings, Matcher<String> ... matchers) {
    for (Matcher<String> m: matchers){
      assertThat(findings, hasItem(m));
    }
  }

  private Set<String> solrXmlIncompatibilities(String level) throws XPathExpressionException, FileNotFoundException {
    return asSet(String.format(INCOMPATIBILITY_DESCRIPTIONS, level), validationResult("solrxml_validation.xml"));
  }

  private Set<String> asSet(String xpath, Path input) throws XPathExpressionException, FileNotFoundException {
    NodeList incompatibilities = (NodeList) XPathFactory.newInstance().newXPath().evaluate(xpath, new InputSource(new FileInputStream(input.toFile())), XPathConstants.NODESET);
    Set<String> incompatibilityList = new HashSet<>();
    for(int i=0;i<incompatibilities.getLength(); i++) {
      Element e = (Element) incompatibilities.item(i);
      incompatibilityList.add(e.getTextContent());
    }
    return incompatibilityList;
  }

  private Path validationResult(String fileName) {
    return upgradedDir.resolve(fileName);
  }

}
