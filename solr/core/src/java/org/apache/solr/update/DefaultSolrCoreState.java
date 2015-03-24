package org.apache.solr.update;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.IOUtils;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultSolrCoreState extends SolrCoreState implements RecoveryStrategy.RecoveryListener {
  public static Logger log = LoggerFactory.getLogger(DefaultSolrCoreState.class);
  
  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");

  private final ReentrantLock recoveryLock = new ReentrantLock();
  
  private final ActionThrottle recoveryThrottle = new ActionThrottle("recovery", 10000);
  
  private final ActionThrottle leaderThrottle = new ActionThrottle("leader", 5000);

  // protects pauseWriter and writerFree
  private final Object writerPauseLock = new Object();
  
  private SolrIndexWriter indexWriter = null;
  private DirectoryFactory directoryFactory;

  private final AtomicInteger recoveryWaiting = new AtomicInteger();

  private volatile RecoveryStrategy recoveryStrat;

  private RefCounted<IndexWriter> refCntWriter;

  private boolean pauseWriter;
  private boolean writerFree = true;
  
  protected final ReentrantLock commitLock = new ReentrantLock();

  private boolean lastReplicationSuccess = true;
  
  // will we attempt recovery as if we just started up (i.e. use starting versions rather than recent versions for peersync
  // so we aren't looking at update versions that have started buffering since we came up.
  private volatile boolean recoveringAfterStartup = true;

  public DefaultSolrCoreState(DirectoryFactory directoryFactory) {
    this.directoryFactory = directoryFactory;
  }
  
  private void closeIndexWriter(IndexWriterCloser closer) {
    try {
      log.info("SolrCoreState ref count has reached 0 - closing IndexWriter");
      if (closer != null) {
        log.info("closing IndexWriter with IndexWriterCloser");
        closer.closeWriter(indexWriter);
      } else if (indexWriter != null) {
        log.info("closing IndexWriter...");
      }
      indexWriter = null;
    } catch (Exception e) {
      log.error("Error during shutdown of writer.", e);
    } 
  }
  
  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core)
      throws IOException {
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCoreState already closed");
      }
      
      while (pauseWriter) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
        }
      }
      
      if (core == null) {
        // core == null is a signal to just return the current writer, or null
        // if none.
        initRefCntWriter();
        if (refCntWriter == null) return null;
        writerFree = false;
        writerPauseLock.notifyAll();
        if (refCntWriter != null) refCntWriter.incref();
        
        return refCntWriter;
      }
      
      if (indexWriter == null) {
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
      }
      initRefCntWriter();
      writerFree = false;
      writerPauseLock.notifyAll();
      refCntWriter.incref();
      return refCntWriter;
    }
  }

  private void initRefCntWriter() {
    if (refCntWriter == null && indexWriter != null) {
      refCntWriter = new RefCounted<IndexWriter>(indexWriter) {
        @Override
        public void close() {
          synchronized (writerPauseLock) {
            writerFree = true;
            writerPauseLock.notifyAll();
          }
        }
      };
    }
  }

  @Override
  public synchronized void newIndexWriter(SolrCore core, boolean rollback) throws IOException {
    log.info("Creating new IndexWriter...");
    String coreName = core.getName();
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      // we need to wait for the Writer to fall out of use
      // first lets stop it from being lent out
      pauseWriter = true;
      // then lets wait until its out of use
      log.info("Waiting until IndexWriter is unused... core=" + coreName);
      
      while (!writerFree) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCoreState already closed");
        }
      }

      try {
        if (indexWriter != null) {
          if (!rollback) {
            closeIndexWriter(coreName);
          } else {
            rollbackIndexWriter(coreName);
          }
        }
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
        log.info("New IndexWriter is ready to be used.");
        // we need to null this so it picks up the new writer next get call
        refCntWriter = null;
      } finally {
        
        pauseWriter = false;
        writerPauseLock.notifyAll();
      }
    }
  }

  private void closeIndexWriter(String coreName) {
    try {
      log.info("Closing old IndexWriter... core=" + coreName);
      Directory dir = indexWriter.getDirectory();
      try {
        IOUtils.closeQuietly(indexWriter);
      } finally {
        if (IndexWriter.isLocked(dir)) {
          IndexWriter.unlock(dir);
        }
      }
    } catch (Exception e) {
      SolrException.log(log, "Error closing old IndexWriter. core="
          + coreName, e);
    }
  }

  private void rollbackIndexWriter(String coreName) {
    try {
      log.info("Rollback old IndexWriter... core=" + coreName);
      Directory dir = indexWriter.getDirectory();
      try {
        
        indexWriter.rollback();
      } finally {
        if (IndexWriter.isLocked(dir)) {
          IndexWriter.unlock(dir);
        }
      }
    } catch (Exception e) {
      SolrException.log(log, "Error rolling back old IndexWriter. core="
          + coreName, e);
    }
  }
  
  @Override
  public synchronized void closeIndexWriter(SolrCore core, boolean rollback)
      throws IOException {
    log.info("Closing IndexWriter...");
    String coreName = core.getName();
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      // we need to wait for the Writer to fall out of use
      // first lets stop it from being lent out
      pauseWriter = true;
      // then lets wait until its out of use
      log.info("Waiting until IndexWriter is unused... core=" + coreName);
      
      while (!writerFree) {
        try {
          writerPauseLock.wait(100);
        } catch (InterruptedException e) {}
        
        if (closed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "SolrCoreState already closed");
        }
      }
      
      if (indexWriter != null) {
        if (!rollback) {
          closeIndexWriter(coreName);
        } else {
          rollbackIndexWriter(coreName);
        }
      }
      
    }
  }
  
  @Override
  public synchronized void openIndexWriter(SolrCore core) throws IOException {
    log.info("Creating new IndexWriter...");
    synchronized (writerPauseLock) {
      if (closed) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Already closed");
      }
      
      try {
        indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
        log.info("New IndexWriter is ready to be used.");
        // we need to null this so it picks up the new writer next get call
        refCntWriter = null;
      } finally {
        pauseWriter = false;
        writerPauseLock.notifyAll();
      }
    }
  }

  @Override
  public synchronized void rollbackIndexWriter(SolrCore core) throws IOException {
    newIndexWriter(core, true);
  }
  
  protected SolrIndexWriter createMainIndexWriter(SolrCore core, String name) throws IOException {
    return SolrIndexWriter.create(name, core.getNewIndexDir(),
        core.getDirectoryFactory(), false, core.getLatestSchema(),
        core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public void doRecovery(final CoreContainer cc, final CoreDescriptor cd) {
    
    Thread thread = new Thread() {
        @Override
        public void run() {
            MDCLoggingContext.setCoreDescriptor(cd);
            try {
            if (SKIP_AUTO_RECOVERY) {
                log.warn(
                        "Skipping recovery according to sys prop solrcloud.skip.autorecovery");
                return;
            }

            // check before we grab the lock
            if (cc.isShutDown()) {
                log.warn("Skipping recovery because Solr is shutdown");
                return;
            }

            // if we can't get the lock, another recovery is running
            // we check to see if there is already one waiting to go
            // after the current one, and if there is, bail
            boolean locked = recoveryLock.tryLock();
            try {
                if (!locked) {
                    if (recoveryWaiting.get() > 0) {
                        return;
                    }
                    recoveryWaiting.incrementAndGet();
                } else {
                    recoveryWaiting.incrementAndGet();
                    cancelRecovery();
                }

                recoveryLock.lock();
                try {
                    recoveryWaiting.decrementAndGet();

                    // to be air tight we must also check after lock
                    if (cc.isShutDown()) {
                        log.warn("Skipping recovery because Solr is shutdown");
                        return;
                    }
                    log.info("Running recovery");

                    recoveryThrottle.minimumWaitBetweenActions();
                    recoveryThrottle.markAttemptingAction();

                    recoveryStrat = new RecoveryStrategy(cc, cd,
                            DefaultSolrCoreState.this);
                    recoveryStrat.setRecoveringAfterStartup(recoveringAfterStartup);
                    Future<?> future = cc.getUpdateShardHandler().getRecoveryExecutor()
                            .submit(recoveryStrat);
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SolrException(ErrorCode.SERVER_ERROR, e);
                    } catch (ExecutionException e) {
                        throw new SolrException(ErrorCode.SERVER_ERROR, e);
                    }
                } finally {
                    recoveryLock.unlock();
                }
            } finally {
                if (locked) recoveryLock.unlock();
            }
        } finally {
                MDCLoggingContext.clear();
            }
    }
    };
    try {
      // we make recovery requests async - that async request may
      // have to 'wait in line' a bit or bail if a recovery is 
      // already queued up - the recovery execution itself is run
      // in another thread on another 'recovery' executor.
      // The update executor is interrupted on shutdown and should 
      // not do disk IO.
      // The recovery executor is not interrupted on shutdown.
      //
      // avoid deadlock: we can't use the recovery executor here
      cc.getUpdateShardHandler().getUpdateExecutor().submit(thread);
    } catch (RejectedExecutionException e) {
    }
  }
  
  @Override
  public void cancelRecovery() {
    if (recoveryStrat != null) {
      try {
        recoveryStrat.close();
      } catch (NullPointerException e) {
        // okay
      }
    }
  }

  /** called from recoveryStrat on a successful recovery */
  @Override
  public void recovered() {
    recoveringAfterStartup = false;  // once we have successfully recovered, we no longer need to act as if we are recovering after startup
  }

  /** called from recoveryStrat on a failed recovery */
  @Override
  public void failed() {}

  @Override
  public synchronized void close(IndexWriterCloser closer) {
    closed = true;
    cancelRecovery();
    closeIndexWriter(closer);
  }
  
  @Override
  public Lock getCommitLock() {
    return commitLock;
  }
  
  @Override
  public ActionThrottle getLeaderThrottle() {
    return leaderThrottle;
  }

  @Override
  public boolean getLastReplicateIndexSuccess() {
    return lastReplicationSuccess;
  }

  @Override
  public void setLastReplicateIndexSuccess(boolean success) {
    this.lastReplicationSuccess = success;
  }
  
}
