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

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.config.upgrade.ConfigType;
import org.apache.solr.config.upgrade.ConfigUpgradeTool;
import org.apache.solr.config.upgrade.ToolParams;
import org.apache.solr.config.upgrade.UpgradeConfigException;

/**
 * Upgrade tool calling wrapper class, calling through local java interface
 */
public class UpgradeToolUtil {
  private static Integer returnValue = 0;

  public static void doUpgradeSchema(Path schemaPath, Path targetSchemaDir) {
    doUpgradeFile(schemaPath, targetSchemaDir, ConfigType.SCHEMA_XML, false);
  }

  public static void doUpgradeSchema(Path schemaPath, Path targetSchemaDir, boolean dryRun) {
    doUpgradeFile(schemaPath, targetSchemaDir, ConfigType.SCHEMA_XML, dryRun);
  }

  public static void doUpgradeConfig(Path configPath, Path targetDir) {
    doUpgradeFile(configPath, targetDir, ConfigType.SOLRCONFIG_XML, false);
  }

  public static void doUpgradeConfig(Path configPath, Path targetDir, boolean dryRun) {
    doUpgradeFile(configPath, targetDir, ConfigType.SOLRCONFIG_XML, dryRun);
  }

  private static void doUpgradeFile(Path schemaPath, Path targetSchemaDir, ConfigType confType, boolean dryRun) {
    URL url = DockerRunner.class.getResource("/solr_4_to_7_processors.xml");
    Path processorXmlPath = Paths.get(url.getPath());
    try {
      ToolParams params = new ToolParams(confType, schemaPath, processorXmlPath, targetSchemaDir, dryRun, false);
      int res = new ConfigUpgradeTool().runTool(params);
      validateReturnCodes(schemaPath, res);
    } catch (Exception e) {
      throw new UpgradeConfigException("Unable to upgrade file");
    }
  }

  private static void validateReturnCodes(Path configPath, int res) {
    Integer ret = returnValue;
    returnValue = 0;
    if (res != 0) {
      throw new UpgradeConfigException("Upgrade of " + configPath.toString() + " failed");
    }
    if (ret != 0) {
      throw new UpgradeConfigException("Upgrade of " + configPath.toString() + " failed during processing");
    }
  }

  public static void init() {
    ConfigUpgradeTool.setExitFunction(i -> returnValue = i );
  }
}
