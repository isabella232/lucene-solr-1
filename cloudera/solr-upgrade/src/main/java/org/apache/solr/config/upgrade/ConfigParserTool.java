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
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.fasterxml.jackson.databind.ObjectMapper;


public class ConfigParserTool {
  public static void main(String[] args) {
    CommandLineParser parser = new PosixParser();
    Options options = new Options();

    options.addOption(null, "list-collections", false,
        "This command lists the collection names configured in the clusterstate.json");
    options.addOption(null, "get-config-name", false,
        "This command retrieves the name of the configset from the collection configuration file"
        + " (stored under /collections/<collectionName> in Zookeeper");
    options.addOption("i", true, "This parameter specifies the path of the Solr configuration file");
    options.addOption("o", true, "This parameter specifies the path of directory where"
        + " the result should be stored.");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("list-collections")) {
        String clusterStateFile = requiredArg(options, cmd, "i");
        listCollections(Paths.get(clusterStateFile));
      } else if (cmd.hasOption("get-config-name")) {
        String collectionConfigFile = requiredArg(options, cmd, "i");
        printConfigName(Paths.get(collectionConfigFile));
      } else {
        System.out.println("unrecognized command");
        System.exit(1);
      }
    } catch (Exception e) {
      System.out.println(e.getLocalizedMessage());
      System.exit(1);
    }

    System.exit(0);
  }

  @SuppressWarnings("unchecked")
  private static void listCollections (Path clusterStateFile) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, ?> result = mapper.readValue(clusterStateFile.toFile(), Map.class);
    for (String o : result.keySet()) {
      System.out.println(o);
    }
  }

  @SuppressWarnings("unchecked")
  private static void printConfigName (Path collectionConfigFile) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, ?> result = mapper.readValue(collectionConfigFile.toFile(), Map.class);
    if (result.containsKey("configName")) {
      System.out.println(result.get("configName"));
    } else {
      throw new IllegalStateException("Unable to find configName property in " + collectionConfigFile);
    }
  }

  private static String requiredArg(Options options, CommandLine cmd, String optVal) {
    if (!cmd.hasOption(optVal)) {
      System.out.println("Please specify the value for option " + optVal);
      System.exit(1);
    }
    return cmd.getOptionValue(optVal);
  }
}
