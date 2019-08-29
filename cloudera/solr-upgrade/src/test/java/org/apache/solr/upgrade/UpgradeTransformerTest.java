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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.CoreMatchers.hasItems;

public class UpgradeTransformerTest extends UpgradeTestBase {
  @After
  public void dumpSolrLogFileIfTestFailed() {
    if(solr4 != null) {
      solr4.dumpLogFileIfPossible();
    }
    if(solr != null) {
      solr.dumpLogFileIfPossible();
    }
  }

  private static final String SIMILARITY_CLASS_XPATH = "/schema/similarity/@class";

  private static final String CONF_DIR = "transformer-test";

  @Test
  public void verifySchemaTransformRules() throws Exception {
    createSolr4Cluster();

    dockerRunner.copy4_10_3SolrXml(new File(solr4.getNodeDir()));
    solr4.start();
    createLegacyCollectionBasedOnConfig(COLLECTION_NAME, CONF_DIR);

    upgradeSchema(CONF_DIR, false);
    upgradeConfig(CONF_DIR, false);

    Set<String> similarityClass = schema();
    assertThat(similarityClass, hasItems("solr.ClassicSimilarityFactory"));
    assertThat(similarityClass, not(hasItems("solr.DefaultSimilarityFactory")));

    stopSolr4();

    createCurrentSolrCluster();
    solr.start();
    createCollectionBasedOnConfig(COLLECTION_NAME, CONF_DIR, upgradedDir);

  }

  @Test
  public void testDoubleConfigTransform() throws Exception {
    Path goodConfigPath = getFile("good_config/solrconfig.xml").toPath();
    Path tmp = createTempDir("double-tmp").toRealPath();
    UpgradeToolUtil.doUpgradeConfig(goodConfigPath, tmp);

    Path transformedConfigPath = tmp.resolve("solrconfig.xml");
    List<String> transformedConfigContent = Files.readAllLines(transformedConfigPath);
    assertTrue("luceneMatchVersion haven't been updated to 7.4! File contents:\n" + transformedConfigContent.toString(),
            transformedConfigContent.stream().anyMatch(s -> s.contains("<luceneMatchVersion>7.4")));

    Path tmp2 = createTempDir("double-tmp2").toRealPath();
    UpgradeToolUtil.doUpgradeConfig(transformedConfigPath, tmp2);

    List<String> doubleTransformedConfigContent = Files.readAllLines(tmp2.resolve("solrconfig.xml"));
    assertTrue("luceneMatchVersion was removed on second conversion! File contents:\n" + doubleTransformedConfigContent.toString(),
            doubleTransformedConfigContent.stream().anyMatch(s -> s.contains("<luceneMatchVersion>7.4")));
  }

  private Set<String> schema() throws XPathExpressionException, FileNotFoundException {
    return getIncompatibilitiesByXPath(schemaTransformationResult());
  }

  private Set<String> getIncompatibilitiesByXPath(Path input) throws XPathExpressionException, FileNotFoundException {
    Node incompatibilities = (Node) XPathFactory.newInstance().newXPath().evaluate(SIMILARITY_CLASS_XPATH, new InputSource(new FileInputStream(input.toFile())), XPathConstants.NODE);
    Set<String> incompatibilityList = new HashSet<>();
    incompatibilityList.add(incompatibilities.getTextContent());
    return incompatibilityList;
  }

  private Path schemaTransformationResult() {
    return upgradedDir.resolve("schema.xml");
  }
}
