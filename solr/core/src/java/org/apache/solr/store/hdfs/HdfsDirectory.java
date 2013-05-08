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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.store.blockcache.CustomBufferedIndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsDirectory extends Directory {
  public static Logger LOG = LoggerFactory.getLogger(HdfsDirectory.class);

  public static final int BUFFER_SIZE = 8192;

  private static final String LF_EXT = ".lf";
  protected static final String SEGMENTS_GEN = "segments.gen";
  protected static final IndexOutput NULL_WRITER = new NullIndexOutput();
  protected Path _hdfsDirPath;
  protected AtomicReference<FileSystem> _fileSystemRef = new AtomicReference<FileSystem>();
  protected Configuration _configuration;

  public HdfsDirectory(Path hdfsDirPath, Configuration configuration) throws IOException {
    assert hdfsDirPath.toString().startsWith("hdfs:/") : hdfsDirPath.toString();
    setLockFactory(NoLockFactory.getNoLockFactory());
    _hdfsDirPath = hdfsDirPath;
    _configuration = configuration;
    reopenFileSystem();
    try {
      if (!getFileSystem().exists(hdfsDirPath)) {
        getFileSystem().mkdirs(hdfsDirPath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Problem creating directory: " + hdfsDirPath, e);
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing hdfs directory {}", _hdfsDirPath);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (SEGMENTS_GEN.equals(name)) {
      return NULL_WRITER;
    }
    HdfsFileWriter writer = new HdfsFileWriter(getFileSystem(), new Path(_hdfsDirPath, name));
    return new HdfsIndexOutput(writer);
  }

  private String[] getNormalNames(List<String> files) {
    int size = files.size();
    for (int i = 0; i < size; i++) {
      String str = files.get(i);
      files.set(i, toNormalName(str));
    }
    return files.toArray(new String[] {});
  }

  private String toNormalName(String name) {
    if (name.endsWith(LF_EXT)) {
      return name.substring(0, name.length() - 3);
    }
    return name;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return openInput(name, BUFFER_SIZE);
  }

  private IndexInput openInput(String name, int bufferSize) throws IOException {
    return new HdfsNormalIndexInput(name, getFileSystem(), new Path(_hdfsDirPath, name), BUFFER_SIZE);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Path path = new Path(_hdfsDirPath, name);
    LOG.debug("Deleting {}", path);
    getFileSystem().delete(path, false);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return getFileSystem().exists(new Path(_hdfsDirPath, name));
  }

  @Override
  public long fileLength(String name) throws IOException {
    return HdfsFileReader.getLength(getFileSystem(), new Path(_hdfsDirPath, name));
  }

  public long fileModified(String name) throws IOException {
    FileStatus fileStatus = getFileSystem().getFileStatus(new Path(_hdfsDirPath, name));
    return fileStatus.getModificationTime();
  }

  @Override
  public String[] listAll() throws IOException {
    FileStatus[] listStatus = getFileSystem().listStatus(_hdfsDirPath);
    List<String> files = new ArrayList<String>();
    if (listStatus == null) {
      return new String[] {};
    }
    for (FileStatus status : listStatus) {
      if (!status.isDirectory()) {
        files.add(status.getPath().getName());
      }
    }
    return getNormalNames(files);
  }

//  @Override
//  public void touchFile(String name) throws IOException {
//    // do nothing
//  }

  public Path getHdfsDirPath() {
    return _hdfsDirPath;
  }

  public FileSystem getFileSystem() {
    return _fileSystemRef.get();
  }
  
  public Configuration getConfiguration() {
    return _configuration;
  }

  protected void reopenFileSystem() throws IOException {
    FileSystem fileSystem = FileSystem.get(_hdfsDirPath.toUri(), _configuration);
    FileSystem oldFs = _fileSystemRef.get();
    _fileSystemRef.set(fileSystem);
    if (oldFs != null) {
      oldFs.close();
    }
  }

//  static class HdfsLayeredIndexInput extends CustomBufferedIndexInput {
//
//    private HdfsFileReader _reader;
//    private long _length;
//    private boolean isClone;
//
//    public HdfsLayeredIndexInput(String name, HdfsFileReader reader, int bufferSize) {
//      super(name, bufferSize);
//      _reader = reader;
//      _length = _reader.length();
//    }
//
//    @Override
//    protected void closeInternal() throws IOException {
//      if (!isClone) {
//        _reader.close();
//      }
//    }
//
//    @Override
//    public long length() {
//      return _length;
//    }
//
//    @Override
//    public Object clone() {
//      HdfsLayeredIndexInput input = (HdfsLayeredIndexInput) super.clone();
//      input.isClone = true;
//      input._reader = (HdfsFileReader) _reader.clone();
//      return input;
//    }
//
//    @Override
//    protected void readInternal(byte[] b, int offset, int length) throws IOException {
//      long position = getFilePointer();
//      _reader.seek(position);
//      _reader.readBytes(b, offset, length);
//    }
//
//    @Override
//    protected void seekInternal(long pos) throws IOException {
//      // do nothing
//    }
//  }

  static class HdfsNormalIndexInput extends CustomBufferedIndexInput {
    public static Logger LOG = LoggerFactory.getLogger(HdfsNormalIndexInput.class);

    private final Path _path;
    private final FSDataInputStream _inputStream;
    private final long _length;
    private boolean _clone = false;

    public HdfsNormalIndexInput(String name, FileSystem fileSystem, Path path, int bufferSize) throws IOException {
      super(name);
      _path = path;
      LOG.debug("Opening normal index input on {}", path);
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      _length = fileStatus.getLen();
      _inputStream = fileSystem.open(path, bufferSize);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      _inputStream.read(getFilePointer(), b, offset, length);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
      _inputStream.seek(pos);
    }

    @Override
    protected void closeInternal() throws IOException {
      LOG.debug("Closing normal index input on {}", _path);
      if (!_clone) {
        _inputStream.close();
      }
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public IndexInput clone() {
      HdfsNormalIndexInput clone = (HdfsNormalIndexInput) super.clone();
      clone._clone = true;
      return clone;
    }
  }

  static class HdfsIndexOutput extends IndexOutput {

    private HdfsFileWriter _writer;

    public HdfsIndexOutput(HdfsFileWriter writer) {
      _writer = writer;
    }

    @Override
    public void close() throws IOException {
      _writer.close();
    }

    @Override
    public void flush() throws IOException {
      _writer.flush();
    }

    @Override
    public long getFilePointer() {
      return _writer.getPosition();
    }

    @Override
    public long length() {
      return _writer.length();
    }

    @Override
    public void seek(long pos) throws IOException {
      _writer.seek(pos);
    }

    @Override
    public void writeByte(byte b) throws IOException {
      _writer.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      _writer.writeBytes(b, offset, length);
    }
  }

//  static class DirectIOHdfsIndexInput extends CustomBufferedIndexInput {
//
//    private long _length;
//    private FSDataInputStream _inputStream;
//    private boolean isClone;
//
//    public DirectIOHdfsIndexInput(String name, FSDataInputStream inputStream, long length) throws IOException {
//      super(name);
//      if (inputStream instanceof DFSDataInputStream) {
//        // This is needed because if the file was in progress of being written
//        // but
//        // was not closed the
//        // length of the file is 0. This will fetch the synced length of the
//        // file.
//        _length = ((DFSDataInputStream) inputStream).getVisibleLength();
//      } else {
//        _length = length;
//      }
//      _inputStream = inputStream;
//    }
//
//    @Override
//    public long length() {
//      return _length;
//    }
//
//    @Override
//    protected void closeInternal() throws IOException {
//      if (!isClone) {
//        _inputStream.close();
//      }
//    }
//
//    @Override
//    protected void seekInternal(long pos) throws IOException {
//
//    }
//
//    @Override
//    protected void readInternal(byte[] b, int offset, int length) throws IOException {
//      synchronized (_inputStream) {
//        _inputStream.readFully(getFilePointer(), b, offset, length);
//      }
//    }
//
//    @Override
//    public IndexInput clone() {
//      DirectIOHdfsIndexInput clone = (DirectIOHdfsIndexInput) super.clone();
//      clone.isClone = true;
//      return clone;
//    }
//  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    LOG.debug("Sync called on {}", Arrays.toString(names.toArray()));
  }
  
}
