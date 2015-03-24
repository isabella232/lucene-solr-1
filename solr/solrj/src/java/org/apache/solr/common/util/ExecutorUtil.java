package org.apache.solr.common.util;

import java.util.ArrayList;
import java.util.List;

import java.util.Collection;

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

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class ExecutorUtil {
  public static Logger log = LoggerFactory.getLogger(ExecutorUtil.class);

  private static volatile List<InheritableThreadLocalProvider> providers = new ArrayList<>();

  public synchronized static void addThreadLocalProvider(InheritableThreadLocalProvider provider) {
    for (InheritableThreadLocalProvider p : providers) {//this is to avoid accidental multiple addition of providers in tests
      if (p.getClass().equals(provider.getClass())) return;
    }
    List<InheritableThreadLocalProvider> copy = new ArrayList<>(providers);
    copy.add(provider);
    providers = copy;
  }

  /** Any class which wants to carry forward the threadlocal values to the threads run
   * by threadpools must implement this interface and the implementation should be
   * registered here
   */
  public interface InheritableThreadLocalProvider {
    /**This is invoked in the parent thread which submitted a task.
     * copy the necessary Objects to the ctx. The object that is passed is same
     * across all three methods
     */
    public void store(AtomicReference<?> ctx);

    /**This is invoked in the Threadpool thread. set the appropriate values in the threadlocal
     * of this thread.     */
    public void set(AtomicReference<?> ctx);

    /**This method is invoked in the threadpool thread after the execution
     * clean all the variables set in the set method
     */
    public void clean(AtomicReference<?> ctx);
  }

  // ** This will interrupt the threads! ** Lucene and Solr do not like this because it can close channels, so only use
  // this if you know what you are doing - you probably want shutdownAndAwaitTermination.
  // Marked as Deprecated to discourage use.
  @Deprecated
  public static void shutdownWithInterruptAndAwaitTermination(ExecutorService pool) {
    pool.shutdownNow(); // Cancel currently executing tasks - NOTE: this interrupts!
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }
  
  // ** This will interrupt the threads! ** Lucene and Solr do not like this because it can close channels, so only use
  // this if you know what you are doing - you probably want shutdownAndAwaitTermination.
  // Marked as Deprecated to discourage use.
  @Deprecated
  public static void shutdownAndAwaitTerminationWithInterrupt(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    boolean shutdown = false;
    boolean interrupted = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
      if (!shutdown && !interrupted) {
        pool.shutdownNow(); // Cancel currently executing tasks - NOTE: this interrupts!
        interrupted = true;
      }
    }
  }
  
  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * See {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)}
   */
  public static ExecutorService newMDCAwareFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  /**
   * See {@link java.util.concurrent.Executors#newSingleThreadExecutor(ThreadFactory)}
   */
  public static ExecutorService newMDCAwareSingleThreadExecutor(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  /**
   * See {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)}
   */
  public static ExecutorService newMDCAwareCachedThreadPool(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory);
  }

  public static class MDCAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final int MAX_THREAD_NAME_LEN = 512;

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public MDCAwareThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    @Override
    public void execute(final Runnable command) {
      final Map<String, String> submitterContext = MDC.getCopyOfContextMap();
      String ctxStr = submitterContext != null && !submitterContext.isEmpty() ?
          submitterContext.toString().replace("/", "//") : "";
      final String submitterContextStr = ctxStr.length() <= MAX_THREAD_NAME_LEN ? ctxStr : ctxStr.substring(0, MAX_THREAD_NAME_LEN);
      super.execute(new Runnable() {
        @Override
        public void run() {
          Map<String, String> threadContext = MDC.getCopyOfContextMap();
          final Thread currentThread = Thread.currentThread();
          final String oldName = currentThread.getName();
          if (submitterContext != null && !submitterContext.isEmpty()) {
            MDC.setContextMap(submitterContext);
            currentThread.setName(oldName + "-processing-" + submitterContextStr);
          } else {
            MDC.clear();
          }
          try {
            command.run();
          } finally {
            if (threadContext != null) {
              MDC.setContextMap(threadContext);
            } else {
              MDC.clear();
            }
            currentThread.setName(oldName);
          }
        }
      });
    }
  }

}
