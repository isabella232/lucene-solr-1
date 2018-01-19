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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Actual upgrade tests
 */
public class DockerUpgradeTest extends UpgradeTestBase {

  @Test
  public void runSolrAfterUpgradingConfigAndSchema() throws Exception {
    assertConfigMigration(CONFIG_NAME_4_10_3);

    addDocument();
    assertThat(queryDocuments().get(0).get("id"), equalTo(ID_VALUE));
  }

  @Test
  public void verifyRemovedFields() throws Exception {
    assertConfigMigration("cloud-removedFields");

    addDocument();
    assertThat(queryDocuments().get(0).get("id"), equalTo(ID_VALUE));
  }

}
