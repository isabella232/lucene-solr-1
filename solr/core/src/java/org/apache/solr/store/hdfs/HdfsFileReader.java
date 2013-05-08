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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.DataInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileReader extends DataInput {

  public static Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);

  private final Path _path;
  private FSDataInputStream _inputStream;
  private long _length;
  private boolean isClone;

  public HdfsFileReader(FileSystem fileSystem, Path path, int bufferSize) throws IOException {
    _path = path;
    LOG.debug("Opening reader on {}", _path);
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.toString());
    }
    _inputStream = fileSystem.open(path, bufferSize);
FileStatus fileStatus = fileSystem.getFileStatus(path);
_length = fileStatus.getLen();
  }

  public HdfsFileReader(FileSystem fileSystem, Path path) throws IOException {
    this(fileSystem, path, HdfsDirectory.BUFFER_SIZE);
  }

//  public long getPosition() throws IOException {
//    return _inputStream.getPos();
//  }

  public long length() {
  return _length;
  }

  public void seek(long pos) throws IOException {
    _inputStream.seek(pos);
  }

  public void close() throws IOException {
    if (!isClone) {
      _inputStream.close();
    }
    LOG.debug("Closing reader on {}", _path);
  }

  /**
   * This method should never be used!
   */
  @Override
  public byte readByte() throws IOException {
    LOG.warn("Should not be used!");
    return _inputStream.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      int lenRead = _inputStream.read(b, offset, len);
      offset += lenRead;
      len -= lenRead;
    }
  }

  public static long getLength(FileSystem fileSystem, Path path) throws IOException {
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      return fileStatus.getLen();
  }

  @Override
  public DataInput clone() {
    HdfsFileReader reader = (HdfsFileReader) super.clone();
    reader.isClone = true;
    return reader;
  }

}
