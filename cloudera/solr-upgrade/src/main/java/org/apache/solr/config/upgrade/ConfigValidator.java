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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.solr.config.upgrade.UpgradeProcessorsConfig.ProcessorConfig;
import org.w3c.dom.Document;

/**
 * TODO
 */
public class ConfigValidator {
  private final String confName;
  private final Optional<Transformer> transformer;
  private Path scriptPath;

  public ConfigValidator(ToolParams params, String confName, ProcessorConfig procConfig) {
    this.confName = confName;
    this.transformer = buildTransformer(params, procConfig);
  }

  public boolean validate(Source config, ValidationHandler handler) {
    if (!transformer.isPresent()) {
      return false;
    }
    try {
      handler.begin(confName);
      DOMResult validationResult = new DOMResult();
      transformer.get().transform(config, validationResult);
      return handler.process(confName, (Document)validationResult.getNode());
    } catch (TransformerException ex) {
      System.out.println("Configuration validation failed.");
      System.out.printf("Unexpected error executing %s : %s", scriptPath, ex.getLocalizedMessage());
      return false;
    }
  }

  protected Optional<Transformer> buildTransformer(ToolParams params, ProcessorConfig procConfig) {
    scriptPath = Paths.get(procConfig.getValidatorPath());
    if (!scriptPath.isAbsolute()) {
      scriptPath = params.getProcessorConfPath().getParent().resolve(scriptPath);
    }
    if (!Files.exists(scriptPath)) {
      throw new IllegalArgumentException("Unable to find validation script "+scriptPath);
    }
    try {
      return Optional.of(params.getFactory().newTransformer(new StreamSource(scriptPath.toFile()))) ;
    } catch (TransformerConfigurationException e) {
      System.out.printf("Following syntactical errors found in script %s : %s", scriptPath, e.getLocalizedMessage());
      System.out.println();
      return Optional.empty();
    }
  }
}
