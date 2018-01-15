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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Helper class to build up a CLI argument list for a docker command
 */
public class DockerCommandBuilder {
  private List<String> commands = new LinkedList<>();
  private Map<String, String> volumes = new HashMap<>();
  private boolean interactive;
  private List<Integer> exposedPorts = new LinkedList<>();

  public static DockerCommandBuilder runCommand() {
    return new DockerCommandBuilder(Arrays.asList("docker", "run"));
  }

  public DockerCommandBuilder(List<String> initial) {
    commands.addAll(initial);
  }

  public DockerCommandBuilder withVolume(String hostDir, String dockerDir) {
    volumes.put(hostDir, dockerDir);
    return this;
  }


  public String[] build() {
    LinkedList<String> res = new LinkedList<>(commands);
    addVolumes(res);
    if(interactive)
      res.add("-i");
    return res.toArray(new String[res.size()]);
  }

  private void addVolumes(LinkedList<String> res) {
    if (!volumes.isEmpty())
      res.add("-v");
    for (Map.Entry<String, String> entry : volumes.entrySet()) {
      res.add(entry.getKey() + ":" + entry.getValue());
    }
  }

  public DockerCommandBuilder withInteractive(boolean interactive) {
    this.interactive = interactive;
    return this;
  }

  public DockerCommandBuilder exposingPort(int port) {
    exposedPorts.add(port);
    return this;
  }
}
