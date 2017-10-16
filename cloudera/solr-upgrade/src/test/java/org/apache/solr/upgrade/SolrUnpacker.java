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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class SolrUnpacker {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public String unPackSolr4(String tarGzAbsolutePath, Path oldSolrDir) throws IOException {
    String subdir = tarGzAbsolutePath.substring(tarGzAbsolutePath.lastIndexOf('/'), tarGzAbsolutePath.indexOf(".tar.gz"));
    String localSolrDir = oldSolrDir.toString() + subdir;
    if (!new File(localSolrDir).exists()) {
      Files.createDirectories(oldSolrDir);
      unpackPreviousSolr(Paths.get(tarGzAbsolutePath), oldSolrDir);
      assertTrue("From directory does not exit or is not a directory:" + localSolrDir, new File(localSolrDir).exists() && new File(localSolrDir).isDirectory());
    }
    return localSolrDir;
  }

  private static void unpackSolr(String packageDir) {
    try {

      Path solrPackDir = Paths.get(packageDir);
      File unzipped = unGzip(Paths.get(packageDir + "solr-7.0.0-SNAPSHOT.tgz").toFile(), solrPackDir.toFile());
      unTar(unzipped, solrPackDir.toFile());
    } catch (IOException | ArchiveException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getWorkingDir() {
    try {
      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(new String[]{"pwd"});
      shell.execute();
      String localDir = shell.getOutput().trim();
      LOG.info("Working directory: {}", localDir);
      return localDir;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void unpackPreviousSolr(Path solrTar, Path targetDir) {
    try {
      File unzipped = unGzip(solrTar.toFile(), targetDir.toFile());
      unTar(unzipped, targetDir.toFile());
    } catch (IOException | ArchiveException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Ungzip an input file into an output file.
   * <p>
   * The output file is created in the output folder, having the same name
   * as the input file, minus the '.gz' extension.
   *
   * @param inputFile the input .gz file
   * @param outputDir the output directory file.
   * @return The {@File} with the ungzipped content.
   * @throws IOException
   * @throws FileNotFoundException
   */
  private static File unGzip(final File inputFile, final File outputDir) throws FileNotFoundException, IOException {

    LOG.info(String.format("Ungzipping %s to dir %s.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath()));

    final File outputFile = new File(outputDir, inputFile.getName().substring(0, inputFile.getName().length() - 3));

    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(inputFile));
    final FileOutputStream out = new FileOutputStream(outputFile);

    IOUtils.copy(in, out);

    in.close();
    out.close();

    return outputFile;
  }

  /**
   * Untar an input file into an output file.
   * <p>
   * The output file is created in the output folder, having the same name
   * as the input file, minus the '.tar' extension.
   *
   * @param inputFile the input .tar file
   * @param outputDir the output directory file.
   * @return The {@link List} of {@link File}s with the untared content.
   * @throws IOException
   * @throws FileNotFoundException
   * @throws ArchiveException
   */
  private static List<File> unTar(final File inputFile, final File outputDir) throws FileNotFoundException, IOException, ArchiveException {

    LOG.info(String.format("Untaring %s to dir %s.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath()));

    final List<File> untaredFiles = new LinkedList<File>();
    final InputStream is = new FileInputStream(inputFile);
    final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
    TarArchiveEntry entry = null;
    while ((entry = debInputStream.getNextTarEntry()) != null) {
      final File outputFile = new File(outputDir, entry.getName());

      if (entry.isDirectory()) {
        LOG.debug(String.format("Attempting to write output directory %s", outputFile.getAbsolutePath()));
        if (!outputFile.exists()) {
          LOG.debug(String.format("Attempting to create output directory %s", outputFile.getAbsolutePath()));
          if (!outputFile.mkdirs()) {
            throw new IllegalStateException(String.format("Couldn't create directory %s", outputFile.getAbsolutePath()));
          }
        }
      } else {
        LOG.debug(String.format("Creating output file %s", outputFile.getAbsolutePath()));
        if (!outputFile.getParentFile().exists()) {
          outputFile.getParentFile().mkdirs();
        }
        final OutputStream outputFileStream = new FileOutputStream(outputFile);
        IOUtils.copy(debInputStream, outputFileStream);
        outputFileStream.close();

        // rwx(user) rwx(group) rwx(other) -> --x--x--x  ->  binary 0100_1001
        int executable = entry.getMode() & 0b0100_1001;
        boolean isExecutable = executable > 0;
        // execute permission can be set only on existing file, so setting it after write
        outputFile.setExecutable(isExecutable, false);

      }
      untaredFiles.add(outputFile);
    }
    debInputStream.close();

    return untaredFiles;
  }
  
  public String getSolPackageDir() {
    String localDir = getWorkingDir();
    return localDir + "/solr/package/";
  }

  public String unpackCurrentSolr() {
    String solrPackageDir = getSolPackageDir();
    String solrToDir = solrPackageDir + "solr-7.0.0-SNAPSHOT";
    if (!new File(solrToDir).exists()) {
      unpackSolr(solrPackageDir);
    }
    return solrToDir;
  }
}
