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
package org.apache.solr.config.upgrade;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Consumer;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.config.upgrade.UpgradeProcessorsConfig.ProcessorConfig;
import org.apache.solr.config.upgrade.impl.DefaultValidationResultProcessor;

/**
 * A configuration compatibility checker tool for Solr. This tool is capable of detecting (and fixing)
 * compatibility errors in the Solr configuration files (specifically schema.xml, solrconfig.xml and
 * solr.xml).
 */
public class ConfigUpgradeTool {
  private static final String DRY_RUN = "dry-run";
  private static final String SOLR_CONF_PATH = "c";
  private static final String UPGRADE_PROCESSORS_CONF_FILE = "u";
  public static final String SOLR_CONFIG_TYPE = "t";
  public static final String RESULT_DIR_PATH = "d";
  
  // TODO: currently this just turns on XSLT compiler warnings, but seems
  // more useful as a verbose flag for debugging - x could be used for XSLT
  // compiler warnings only if desired.
  public static final String VERBOSE_OUTPUT = "v";
  
  private static Consumer<Integer> exitFunction = System::exit;

  public static void setExitFunction(Consumer<Integer> f) {
    exitFunction = f;
  }
  public static void main(String[] args) {
    CommandLineParser parser = new PosixParser();
    Options options = new Options();

    options.addOption(null, DRY_RUN, false, "This command will perform compatibility checks for the specified Solr config file.");
    options.addOption(UPGRADE_PROCESSORS_CONF_FILE, true, "This parameter specifies the path of the file providing Solr upgrade processor configurations.");
    options.addOption(SOLR_CONF_PATH, true, "This parameter specifies the path of Solr configuration to be upgraded.");
    options.addOption(SOLR_CONFIG_TYPE, true, "This parameter specifies the type of Solr configuration to be validated and transformed."
        + "The tool currently supports schema.xml, solrconfig.xml and solr.xml");
    options.addOption(RESULT_DIR_PATH, true,
        "This parameter specifies the directory path where tansformed Solr configuration should be stored.");
    options.addOption(VERBOSE_OUTPUT, false, "This parameter enables printing XSLT compiler warnings on the command output.");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      boolean dryRun = cmd.hasOption(DRY_RUN);
      Path processorConfPath = Paths.get(requiredArg(options, cmd, UPGRADE_PROCESSORS_CONF_FILE));
      ConfigType confType = ConfigType.getConfigType(requiredArg(options, cmd, SOLR_CONFIG_TYPE).toLowerCase());
      Path solrConfPath = Paths.get(requiredArg(options, cmd, SOLR_CONF_PATH));

      String resultDirPathStr = cmd.getOptionValue(RESULT_DIR_PATH);
      if (!dryRun && (resultDirPathStr == null)) {
        System.out.println("Please specify the value for option -d");
        exitFunction.accept(1);
      }
      Path resultDirPath = (resultDirPathStr != null) ? Paths.get(resultDirPathStr) : null;

      boolean verbose = cmd.hasOption(VERBOSE_OUTPUT);

      ToolParams params = new ToolParams(confType, solrConfPath, processorConfPath, resultDirPath, dryRun, verbose);

      exitFunction.accept((new ConfigUpgradeTool()).runTool(params));

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getLocalizedMessage());
      exitFunction.accept(1);
    }
  }

  /**
   * Invoke the compatibility checker
   *
   * @throws Exception in case of errors
   */
  public int runTool(ToolParams params) throws Exception {
    params.checkArgs();

    boolean result = true;
    ValidationHandler handler = new DefaultValidationResultProcessor(params);
    Optional<ConfigValidator> validator = getConfigValidator(params);
    if (validator.isPresent()) {
      result = validator.get().validate(getConfigSource(params), handler);
      // Note we expect the transformation rules to be present in the validation script
      // at an info level. When validation script is not present, there is no need for
      // transformation as well.
      if (result && !params.isDryRun()) {
        Optional<ConfigTransformer> transformer = getConfigTransformer(params);
        if (transformer.isPresent()) {
          transformer.get().transform(getConfigSource(params));
        }
      } else {
          System.out.println(
              "No transform of the input file was done because you specified a dry run or because there are errors in the file that need to be manually addressed.");
      }
    } else {
      System.out.println("No validation rules found for config type : " + params.getConfType()
      + " in processor configuration " + params.getProcessorConfPath() );
    }

    return result ? 0 : 1;
  }

  private Source getConfigSource(ToolParams params) {
    ConfigType confType = params.getConfType();
    switch (confType) {
      case SCHEMA_XML:
      case SOLRCONFIG_XML:
      case SOLR_XML: {
        return new StreamSource(params.getSolrConfPath().toFile());
      }

      case CONFIGSET: {
        throw new UnsupportedOperationException();
      }
      default: {
        throw new UnsupportedOperationException();
      }
    }
  }

  private Optional<ConfigValidator> getConfigValidator(ToolParams params) {
    ConfigType confType = params.getConfType();
    UpgradeProcessorsConfig conf = params.getUpgradeProcessorConf();

    switch (confType) {
      case SCHEMA_XML:
      case SOLRCONFIG_XML:
      case SOLR_XML: {
        Optional<ProcessorConfig> procConf = conf.getProcessorByConfigType(confType);
        if (procConf.isPresent() && (procConf.get().getValidatorPath() != null)) {
          return Optional.of(new ConfigValidator(params, confType.getConfigType(), procConf.get()));
        }
        break;
      }

      case CONFIGSET: {
        throw new UnsupportedOperationException();
      }
    }

    return Optional.empty();
  }

  private Optional<ConfigTransformer> getConfigTransformer(ToolParams params) {
    ConfigType confType = params.getConfType();
    UpgradeProcessorsConfig conf = params.getUpgradeProcessorConf();

    switch (confType) {
      case SCHEMA_XML:
      case SOLRCONFIG_XML:
      case SOLR_XML: {
        Optional<ProcessorConfig> procConf = conf.getProcessorByConfigType(confType);
        if (procConf.isPresent()) {
          return Optional.of(new ConfigTransformer(params, procConf.get()));
        }
        break;
      }

      case CONFIGSET: {
        throw new UnsupportedOperationException();
      }
    }

    return Optional.empty();
  }

  private static String requiredArg(Options options, CommandLine cmd, String optVal) {
    if (!cmd.hasOption(optVal)) {
      System.out.println("Please specify the value for option " + optVal);
      exitFunction.accept(1);
    }
    return cmd.getOptionValue(optVal);
  }
}
