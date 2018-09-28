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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import com.google.common.io.Files;
import org.apache.solr.config.upgrade.UpgradeConfigException;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;

import static org.apache.solr.util.RegexMatcher.matchesPattern;
import static org.hamcrest.core.IsNot.not;
import static org.junit.matchers.JUnitMatchers.everyItem;
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

  @Test
  public void verifyMissingBackupTag() throws Exception {
    String solrXmlFilename = "solr-4.10.3-compat.xml";
    startSolrWithXml(solrXmlFilename);
    Path solrXml = Paths.get(TEST_HOME(), solrXmlFilename);
    UpgradeToolUtil.doUpgradeSolrXml(solrXml, upgradedDir, false);
    LOG.info(Files.toString(upgradedDir.resolve(solrXmlFilename).toFile(), Charset.forName("UTF-8")));
    assertMatchesAll(solrXmlIncompatibilities("info"),
        matchesPattern("must contain HDFS backup repository definition")
    );
  }

  @Test
  public void verifyExistingBackupTag() throws Exception {
    String solrXmlFilename = "solr-4.10.3-compat-hasbackup.xml";
    startSolrWithXml(solrXmlFilename);
    Path solrXml = Paths.get(TEST_HOME(), solrXmlFilename);
    UpgradeToolUtil.doUpgradeSolrXml(solrXml, upgradedDir, false);
    LOG.info(Files.toString(upgradedDir.resolve(solrXmlFilename).toFile(), Charset.forName("UTF-8")));
    LOG.info(Files.toString(upgradedDir.resolve("solrxml_validation.html").toFile(), Charset.forName("UTF-8")));

    assertThat(solrXmlIncompatibilities("info"), everyItem(not(matchesPattern("must contain HDFS backup repository definition"))));
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

  private Set<String> solrXmlIncompatibilities(String level) throws IOException {
    return getIncompatibilitiesByQuery(String.format(INCOMPATIBILITY_DESCRIPTIONS, level), validationResult("solrxml_validation.html"));
  }


  private Path validationResult(String fileName) {
    return upgradedDir.resolve(fileName);
  }

}
