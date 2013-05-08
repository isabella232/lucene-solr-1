package org.apache.solr.store.hdfs;

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockReleaseFailedException;
import org.apache.solr.util.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsLockFactory extends LockFactory {
  
  private Path lockPath;
  private Configuration configuration;


  public HdfsLockFactory(Path lockPath, Configuration configuration) {
    this.lockPath = lockPath;
    this.configuration = configuration;
  }
  
  @Override
  public Lock makeLock(String lockName) {
    
    FileSystem fs;
    try {
      fs = FileSystem.get(lockPath.toUri(), configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    
    HdfsLock lock = new HdfsLock(fs, lockPath, lockName);
    
    return lock;
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    FileSystem fs;
    try {
      fs = FileSystem.get(lockPath.toUri(), configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    if (fs.exists(lockPath)) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
      
      Path lockFile = new Path(lockPath, lockName);
      if (fs.exists(lockFile) && !fs.delete(lockFile, false));
    }
  }
  
  public Path getLockPath() {
    return lockPath;
  }

  public void setLockPath(Path lockPath) {
    this.lockPath = lockPath;
  }
  
  static class HdfsLock extends Lock {

    private FileSystem fs;
    private Path lockPath;
    private String lockName;

    public HdfsLock(FileSystem fs, Path lockPath, String lockName) {
      this.fs = fs;
      this.lockPath = lockPath;
      this.lockName = lockName;
    }
    
    @Override
    public boolean obtain() throws IOException {
      FSDataOutputStream file = null;
      try {
        file = fs.create(new Path(lockPath, lockName), false);
      } catch (IOException e) {
        return false;
      } finally {
        if (file != null) {
          file.close();
        }
      }
      return true;
    }

    @Override
    public void release() throws IOException {
      if (fs.exists(new Path(lockPath, lockName)) && !fs.delete(new Path(lockPath, lockName), false))
        throw new LockReleaseFailedException("failed to delete " + new Path(lockPath, lockName));
    }

    @Override
    public boolean isLocked() throws IOException {
      return fs.exists(new Path(lockPath, lockName));
    }
    
  }
  
}
