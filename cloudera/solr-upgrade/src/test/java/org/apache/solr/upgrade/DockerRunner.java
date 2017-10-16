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

import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.PortBinding;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkCLI;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import static com.spotify.docker.client.DockerClient.RemoveContainerParam.forceKill;

public class DockerRunner {
  public static final int SOLR_PORT_NUMBER = 8983;
  public static final String SOLR_STOP_PORT = String.valueOf(SOLR_PORT_NUMBER - 1000);
  public static final String SOLR_PORT = String.valueOf(SOLR_PORT_NUMBER);
  static final String ZK_PORT = "2182";
  public static final ImmutableMap<String, List<PortBinding>> ZK_PORT_BINDING = ImmutableMap.of(
      ZK_PORT, Arrays.asList(PortBinding.of("0.0.0.0", ZK_PORT))
  );
  static final String HDFS_NAMENODE_PORT = "50070";
  public static final ImmutableMap<String, List<PortBinding>> SOLR_PORT_BINDINGS = ImmutableMap.of(SOLR_PORT, Arrays.asList(PortBinding.of("0.0.0.0", SOLR_PORT)));
  public static final String SOLR_FROM = "/solr-from";
  public static final String SOLR_TO = "/solr-to";
  public static final String DOCKER_LABEL = "docker-upgrade";
  public static final String CORES_SUB_DIR = "/cores";
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final String ZK_DIR = "/zk-working-dir";
  public static final String[] NEVER_ENDING_COMMAND = {"tail", "-f", "/dev/null"};
  public static final String IMAGE_WITH_SOLR5_AND_6 = "with-solr5n6";
  public static final String IMAGE_SOLR_ALL = "solr-all-versions";

  // details: https://bitbucket.org/uhopper/hadoop-docker/overview
  public static final String NAMENODE_IMAGE_NAME = "uhopper/hadoop-namenode:2.7.2";
  public static final String DATANODE_IMAGE_NAME = "uhopper/hadoop-datanode:2.7.2";
  public static final String DOCKER_FILES_DIR = "/solr/dockerfiles/";
  private static Set<DockerClient> allClients = new HashSet<>();
  private final Context context;

  public DockerRunner(Context context) {
    this.context = context;
  }

  public static void removeAllDockerContainers() throws DockerException, InterruptedException {
    DockerClient docker = dockerClient();
    // containers with label "docker-upgrade"
    DockerClient.ListContainersParam upgradeLabelFilter = DockerClient.ListContainersParam.withLabel(DOCKER_LABEL, "");
    // show exited containers too
    DockerClient.ListContainersParam allContainer = DockerClient.ListContainersParam.allContainers();

    List<Container> dockerUpgradeContainers = docker.listContainers(upgradeLabelFilter, allContainer);
    dockerUpgradeContainers.forEach(c -> {
      killContainerSync(docker, c);
    });
    closeDockerClientSilently(docker);
  }

  public void copy4_10_3SolrXml(File dstRoot) throws IOException {
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), "solr-4.10.3-compat.xml");
    FileUtils.copyFile(xmlF, new File(dstRoot, "solr.xml"));
  }


  public void uploadConfig(Path configDir, String configName) throws IOException {
    try {
      ZkCLI.main(new String[]{"-zkhost", "localhost:"+ DockerRunner.ZK_PORT +"/solr" , "-cmd", "upconfig", "-confdir", configDir.toString(), "-confname",configName});
    } catch (InterruptedException | TimeoutException | SAXException | ParserConfigurationException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void downloadConfig(String configName, Path targetDir) throws IOException {
    try {
      ZkCLI.main(new String[]{"-zkhost", "localhost:"+ DockerRunner.ZK_PORT +"/solr", "-cmd", "downconfig", "-confdir", targetDir.toString(), "-confname", configName});
    } catch (InterruptedException | TimeoutException | SAXException | ParserConfigurationException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  private static void killContainerSync(DockerClient docker, Container c) {
    try {
      LOG.info("Removing container {} having state/status : {}/{}", c.names(), c.state(), c.status());
      if("running".equals(c.state())) {
        docker.killContainer(c.id());
        docker.waitContainer(c.id());
      }
      docker.removeContainer(c.id(), forceKill());
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void closeClient(DockerClient docker) {
    allClients.remove(docker);
    docker.close();
  }

  public SolrRunner solrRunner() {
    return new SolrRunner(context);
  }

  public Solr4CloudRunner solr4CloudRunner(ZooKeeperRunner zooKeeper) {
    return new Solr4CloudRunner(zooKeeper, context);
  }


  private void buildImageWithPreviousSolrVersions() {
    String imageDir = System.getProperty("solr.5.docker.image", "solr5");
    buildImage(imageDir, IMAGE_WITH_SOLR5_AND_6);
  }

  public void buildImage(String imageDir, String imageName) {
    DockerClient docker = dockerClient();
    try {
      String resource = DOCKER_FILES_DIR + imageDir;
      String pathString = DockerRunner.class.getResource(resource).getPath();
      LOG.debug("dockerfile directory: {}", pathString);
      Path path = Paths.get(pathString);
      docker.build(path, imageName);
    } catch (DockerException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      closeClient(docker);
    }
  }

  public SolrCloudRunner solrCloudRunner(ZooKeeperRunner zooKeeper) {
    return new SolrCloudRunner(zooKeeper, context);
  }

  public UpgradeToolFacade upgradeToolFacade() {
    return new UpgradeToolFacade(context);
  }

  private Path copyToTempSchemaFile(String schema) {
    try {
      Path dir = Files.createTempDirectory("tmp-schema").toRealPath();
      Path res = Paths.get(dir.toString(), "schema.xml");
      FileUtils.copyToFile(new ByteArrayInputStream(schema.getBytes()), res.toFile());
      return res;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Solr4Runner solr4Runner() {
    return new Solr4Runner(context);
  }

  public ZooKeeperRunner zooKeeperRunner() {
    return new ZooKeeperRunner(context);
  }

  public HdfsRunner hdfsRunner() {
    return new HdfsRunner(context);
  }


  public static String LINUX_JAVA_IMAGE_NAME = "anapsix/alpine-java:latest";


  public static DockerClient dockerClient() {
    try {
      // Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
      final DockerClient docker = DefaultDockerClient.fromEnv().build();
      // Pull an image
      List<Image> alpineJavaImages = docker.listImages(DockerClient.ListImagesParam.byName(LINUX_JAVA_IMAGE_NAME));
      if (alpineJavaImages.isEmpty()) {
        docker.pull(LINUX_JAVA_IMAGE_NAME);
      }

      List<Image> nameNodeImage = docker.listImages(DockerClient.ListImagesParam.byName(NAMENODE_IMAGE_NAME));
      if (nameNodeImage.isEmpty()) {
        docker.pull(NAMENODE_IMAGE_NAME);
      }

      List<Image> dataNodeImage = docker.listImages(DockerClient.ListImagesParam.byName(DATANODE_IMAGE_NAME));
      if (dataNodeImage.isEmpty()) {
        docker.pull(DATANODE_IMAGE_NAME);
      }
      allClients.add(docker);
      return docker;
    } catch (DockerCertificateException | DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void closeAllClients() {
    allClients.forEach(DockerRunner::closeDockerClientSilently);
  }

  private static void closeDockerClientSilently(DockerClient dockerClient) {
    try {
      dockerClient.close();
    } catch(Exception ignored) {
    }
  }


  public static class Context {
    public String namePrefix;
    public DockerClient docker = dockerClient();

    public Context withContainerNamePrefix(String namePrefix) {
      this.namePrefix = namePrefix;
      return this;
    }

    public DockerRunner build() {
      return new DockerRunner(this);
    }

    public DockerClient getDocker() {
      return docker;
    }

    public void close() {
      closeClient(docker);
    }
  }

  public void buildImages() {
    buildImage("solr_all", IMAGE_SOLR_ALL);
    buildImageWithPreviousSolrVersions();
  }

}
