package org.apache.solr.util;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class HdfsUtil {
  public static Logger LOG = LoggerFactory
      .getLogger(HdfsUtil.class);
  
  // safety override
  public static final int NN_SAFEMODE_RETRY_TIME = Integer.getInteger(
      "solr.nnsafemode.retrytime", 5000);
  
  // safety override
  public static final int NN_SAFEMODE_TIMEOUT = Integer.getInteger(
      "solr.nnsafemode.timeout", 3600000);
  
  private static final String[] HADOOP_CONF_FILES = {"core-site.xml",
      "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};
  
  public static void addHdfsResources(Configuration conf, String confDir) {
    if (confDir != null && confDir.length() > 0) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Resource directory does not exist: "
                + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.isDirectory()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Specified resource directory is not a directory"
                + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.canRead()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Resource directory must be readable by the Solr process: "
                + confDirFile.getAbsolutePath());
      }
      for (String file : HADOOP_CONF_FILES) {
        if (new File(confDirFile, file).exists()) {
          conf.addResource(new Path(confDir, file));
        }
      }
    }
  }
  
  public static void mkDirIfNeededAndWaitForSafeMode(FileSystem fileSystem,
      Path hdfsDirPath) {
    long now = System.currentTimeMillis();
    long timeout = now + NN_SAFEMODE_TIMEOUT;
    
    while (true) {

      
      try {
        if (!fileSystem.exists(hdfsDirPath)) {
          fileSystem.mkdirs(hdfsDirPath);
          // if we checked the return of mkdirs, the
          // dir could exist in a race, which is fine
          if (!fileSystem.exists(hdfsDirPath)) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Could not create directory: "
                + hdfsDirPath);
          }
        } else {
          fileSystem.mkdirs(hdfsDirPath); // check for safe mode
        }
        
        break;
      } catch (RemoteException e) {
        if (e.getClassName() != null && e.getClassName().equals(SafeModeException.class.getName())) {
          LOG.warn("The NameNode is in SafeMode - Solr will wait "
              + Math.round(NN_SAFEMODE_RETRY_TIME / 1000.0)
              + " seconds and try again.");
          try {
            Thread.sleep(NN_SAFEMODE_RETRY_TIME);
          } catch (InterruptedException e1) {
            Thread.interrupted();
          }
          
          if (System.currentTimeMillis() > timeout) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out waiting for the NameNode to leave SafeMode");
          }
          continue;
        }
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Problem creating directory: " + hdfsDirPath, e);
      } catch (Exception e) {
        
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Problem creating directory: " + hdfsDirPath, e);
      }
    }
  }
}
