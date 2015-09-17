package org.apache.solr.common.util;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorUtil {
  public static Logger log = LoggerFactory.getLogger(ExecutorUtil.class);
  
  private static volatile List<InheritableThreadLocalProvider> providers = new ArrayList<InheritableThreadLocalProvider>();

  public synchronized static void addThreadLocalProvider(InheritableThreadLocalProvider provider) {
    for (InheritableThreadLocalProvider p : providers) {//this is to avoid accidental multiple addition of providers in tests
      if (p.getClass().equals(provider.getClass())) return;
    }
    List<InheritableThreadLocalProvider> copy = new ArrayList<InheritableThreadLocalProvider>(providers);
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
        shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }
}
