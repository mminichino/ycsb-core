/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.codelry.util.ycsb;

import com.codelry.util.ycsb.measurements.Measurements;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages YCSB client worker threads, including optional variable thread-count control
 * (sync bindings) or variable concurrency control (reactive bindings).
 *
 * <p>Sync path: variable mode when {@code threadcount=0}. The pool starts at
 * {@code 4 * availableProcessors} (clamped to min/max), then periodically adjusts:
 * {@code new_limit = current_limit * (min_latency / current_window_latency)}.
 *
 * <p>Reactive path (binding extends {@link DBReactive}): worker threads are fixed
 * (CPU count by default; explicit {@code threadcount > 0} overrides; {@code threadcount=0}
 * means default CPU count — variable threads are not supported). In-flight operations are
 * limited by {@code concurrency} (default 256). {@code concurrency=0} enables variable
 * concurrency using the same latency-gradient controller as variable threads.
 */
public class ClientThreadPool {
  private static final Logger logger = LoggerFactory.getLogger(ClientThreadPool.class);

  public static final String MIN_THREAD_COUNT_PROPERTY = "threadcount.min";
  public static final String MAX_THREAD_COUNT_PROPERTY = "threadcount.max";
  public static final String THREAD_ADJUST_INTERVAL_PROPERTY = "threadcount.adjustinterval";
  public static final String MIN_LATENCY_PROPERTY = "threadcount.minlatency";

  public static final String CONCURRENCY_PROPERTY = "concurrency";
  public static final String MIN_CONCURRENCY_PROPERTY = "concurrency.min";
  public static final String MAX_CONCURRENCY_PROPERTY = "concurrency.max";
  public static final String CONCURRENCY_ADJUST_INTERVAL_PROPERTY = "concurrency.adjustinterval";
  public static final String CONCURRENCY_MIN_LATENCY_PROPERTY = "concurrency.minlatency";

  public static final String THREAD_ADJUST_INTERVAL_DEFAULT = "10";
  public static final String MAX_THREAD_COUNT_DEFAULT = "256";
  public static final String MIN_LATENCY_MS_DEFAULT = "1";
  public static final String CONCURRENCY_DEFAULT = "256";
  public static final String MAX_CONCURRENCY_DEFAULT = "256";

  private final String dbname;
  private final Properties props;
  private final Workload workload;
  private final Tracer tracer;
  private final boolean dotransactions;
  private final boolean reactiveMode;
  private final boolean variableMode;
  private final boolean variableConcurrencyMode;
  private final int minThreads;
  private final int maxThreads;
  private final int minConcurrency;
  private final int maxConcurrency;
  private final long adjustIntervalMs;
  private final double minLatencyUs;
  private final int totalOpcount;
  private final int targetOpsPerSec;
  private final AtomicLong sharedOpsRemaining;
  private final int threadcountForWorkload;
  private final ConcurrencyLimiter concurrencyLimiter;

  private final List<ClientThread> clients = Collections.synchronizedList(new ArrayList<>());
  private final List<ReactiveClientThread> reactiveClients = Collections.synchronizedList(new ArrayList<>());
  private final ConcurrentHashMap<Thread, Object> threads = new ConcurrentHashMap<>();
  private final AtomicInteger nextThreadId = new AtomicInteger(0);
  private final AtomicInteger activeWorkers = new AtomicInteger(0);
  private final CountDownLatch completeLatch = new CountDownLatch(1);

  private volatile int currentLimit;
  private volatile int currentConcurrency;
  private volatile boolean acceptingWork = true;
  private Thread adjusterThread;

  private int minClientThreads = Integer.MAX_VALUE;
  private int maxClientThreads;
  private long threadSampleSum;
  private long threadSampleCount;

  private int minClientConcurrency = Integer.MAX_VALUE;
  private int maxClientConcurrency;
  private long concurrencySampleSum;
  private long concurrencySampleCount;

  public ClientThreadPool(String dbname, Properties props, Workload workload, Tracer tracer,
                          int configuredThreadcount, int totalOpcount, int targetOpsPerSec) {
    this.dbname = dbname;
    this.props = props;
    this.workload = workload;
    this.tracer = tracer;
    this.dotransactions = Boolean.parseBoolean(props.getProperty(Client.DO_TRANSACTIONS_PROPERTY, "true"));
    this.totalOpcount = totalOpcount;
    this.targetOpsPerSec = targetOpsPerSec;
    this.reactiveMode = DBFactory.isReactiveBinding(dbname);

    int cores = Math.max(1, Runtime.getRuntime().availableProcessors());

    if (reactiveMode) {
      // Fixed worker threads; variable thread count is not supported.
      this.variableMode = false;
      this.minThreads = 1;
      this.maxThreads = Integer.MAX_VALUE;
      if (!props.containsKey(Client.THREAD_COUNT_PROPERTY) || configuredThreadcount <= 0) {
        if (configuredThreadcount == 0 && props.containsKey(Client.THREAD_COUNT_PROPERTY)) {
          logger.info("Reactive binding: threadcount=0 is ignored (variable threads unsupported); "
              + "using CPU count ({})", cores);
        }
        currentLimit = cores;
      } else {
        currentLimit = configuredThreadcount;
      }
      if (currentLimit > totalOpcount && totalOpcount > 0) {
        currentLimit = totalOpcount;
        logger.warn("Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
      }
      threadcountForWorkload = currentLimit;
      sharedOpsRemaining = null;

      int configuredConcurrency = Integer.parseInt(
          props.getProperty(CONCURRENCY_PROPERTY, CONCURRENCY_DEFAULT));
      this.variableConcurrencyMode = configuredConcurrency == 0;
      this.minConcurrency = Math.max(1, Integer.parseInt(
          props.getProperty(MIN_CONCURRENCY_PROPERTY, String.valueOf(cores))));
      this.maxConcurrency = Math.max(minConcurrency, Integer.parseInt(
          props.getProperty(MAX_CONCURRENCY_PROPERTY, MAX_CONCURRENCY_DEFAULT)));
      this.adjustIntervalMs = TimeUnit.SECONDS.toMillis(Integer.parseInt(
          props.getProperty(CONCURRENCY_ADJUST_INTERVAL_PROPERTY,
              props.getProperty(THREAD_ADJUST_INTERVAL_PROPERTY, THREAD_ADJUST_INTERVAL_DEFAULT))));
      this.minLatencyUs = Double.parseDouble(
          props.getProperty(CONCURRENCY_MIN_LATENCY_PROPERTY,
              props.getProperty(MIN_LATENCY_PROPERTY, MIN_LATENCY_MS_DEFAULT))) * 1000.0;

      if (variableConcurrencyMode) {
        currentConcurrency = clamp(4 * cores, minConcurrency, maxConcurrency);
      } else {
        currentConcurrency = Math.max(1, configuredConcurrency);
      }
      concurrencyLimiter = new ConcurrencyLimiter(currentConcurrency);
    } else {
      this.variableConcurrencyMode = false;
      this.minConcurrency = 1;
      this.maxConcurrency = 1;
      this.currentConcurrency = 1;
      this.concurrencyLimiter = null;

      this.variableMode = configuredThreadcount == 0;
      this.minThreads = Math.max(1, Integer.parseInt(
          props.getProperty(MIN_THREAD_COUNT_PROPERTY, String.valueOf(cores))));
      this.maxThreads = Math.max(minThreads, Integer.parseInt(
          props.getProperty(MAX_THREAD_COUNT_PROPERTY, MAX_THREAD_COUNT_DEFAULT)));
      this.adjustIntervalMs = TimeUnit.SECONDS.toMillis(Integer.parseInt(
          props.getProperty(THREAD_ADJUST_INTERVAL_PROPERTY, THREAD_ADJUST_INTERVAL_DEFAULT)));
      this.minLatencyUs = Double.parseDouble(props.getProperty(MIN_LATENCY_PROPERTY, MIN_LATENCY_MS_DEFAULT)) * 1000.0;

      if (variableMode) {
        currentLimit = clamp(4 * cores, minThreads, maxThreads);
        threadcountForWorkload = maxThreads;
        sharedOpsRemaining = totalOpcount > 0 ? new AtomicLong(totalOpcount) : null;
      } else {
        currentLimit = configuredThreadcount;
        if (configuredThreadcount > totalOpcount && totalOpcount > 0) {
          currentLimit = totalOpcount;
          logger.warn("Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
        }
        threadcountForWorkload = currentLimit;
        sharedOpsRemaining = null;
      }
    }
  }

  public boolean isReactiveMode() {
    return reactiveMode;
  }

  public boolean isVariableMode() {
    return variableMode;
  }

  public boolean isVariableConcurrencyMode() {
    return variableConcurrencyMode;
  }

  public int getCurrentLimit() {
    return currentLimit;
  }

  public int getCurrentConcurrency() {
    return currentConcurrency;
  }

  public List<ClientThread> getClients() {
    return clients;
  }

  public Collection<Thread> getThreads() {
    return threads.keySet();
  }

  public CountDownLatch getCompleteLatch() {
    return completeLatch;
  }

  public long getOpsTodo() {
    if (sharedOpsRemaining != null) {
      return Math.max(0L, sharedOpsRemaining.get());
    }
    long todo = 0;
    if (reactiveMode) {
      synchronized (reactiveClients) {
        for (ReactiveClientThread t : reactiveClients) {
          todo += t.getOpsTodo();
        }
      }
    } else {
      synchronized (clients) {
        for (ClientThread t : clients) {
          todo += t.getOpsTodo();
        }
      }
    }
    return todo;
  }

  public long getOpsDone() {
    long done = 0;
    if (reactiveMode) {
      synchronized (reactiveClients) {
        for (ReactiveClientThread t : reactiveClients) {
          done += t.getOpsDone();
        }
      }
    } else {
      synchronized (clients) {
        for (ClientThread t : clients) {
          done += t.getOpsDone();
        }
      }
    }
    return done;
  }

  public int getMinClientThreads() {
    return minClientThreads == Integer.MAX_VALUE ? 0 : minClientThreads;
  }

  public int getMaxClientThreads() {
    return maxClientThreads;
  }

  public double getAverageClientThreads() {
    if (threadSampleCount == 0) {
      return currentLimit;
    }
    return ((double) threadSampleSum) / ((double) threadSampleCount);
  }

  public int getMinClientConcurrency() {
    return minClientConcurrency == Integer.MAX_VALUE ? 0 : minClientConcurrency;
  }

  public int getMaxClientConcurrency() {
    return maxClientConcurrency;
  }

  public double getAverageClientConcurrency() {
    if (concurrencySampleCount == 0) {
      return currentConcurrency;
    }
    return ((double) concurrencySampleSum) / ((double) concurrencySampleCount);
  }

  /**
   * Create and start the initial worker threads (and the adjuster when in variable mode).
   */
  public synchronized void start() throws UnknownDBException {
    logPhaseSettings();
    sampleClientThreads();
    if (reactiveMode) {
      sampleClientConcurrency();
      spawnFixedReactiveWorkers();
      if (variableConcurrencyMode) {
        adjusterThread = new Thread(this::runConcurrencyAdjuster, "ConcurrencyAdjuster");
        adjusterThread.setDaemon(true);
        adjusterThread.start();
      }
    } else {
      if (variableMode) {
        spawnWorkers(currentLimit);
      } else {
        spawnFixedWorkers();
      }
      if (variableMode) {
        adjusterThread = new Thread(this::runAdjuster, "ThreadCountAdjuster");
        adjusterThread.setDaemon(true);
        adjusterThread.start();
      }
    }
  }

  /**
   * Log client thread / concurrency settings for the phase about to run (load or transactions).
   */
  private void logPhaseSettings() {
    String phase = dotransactions ? "transactions" : "load";
    logger.info("Starting {} phase", phase);
    logger.info("Client threads: {}", currentLimit);
    if (variableMode) {
      logger.info("Variable thread count: start={}, min={}, max={}, adjustInterval={}s, minLatency={}ms",
          currentLimit, minThreads, maxThreads, adjustIntervalMs / 1000, minLatencyUs / 1000.0);
    }
    if (reactiveMode) {
      if (variableConcurrencyMode) {
        logger.info("Variable concurrency: start={}, min={}, max={}, adjustInterval={}s, minLatency={}ms",
            currentConcurrency, minConcurrency, maxConcurrency,
            adjustIntervalMs / 1000, minLatencyUs / 1000.0);
      } else {
        logger.info("Concurrency: {}", currentConcurrency);
      }
    }
  }

  /**
   * Wait for all workers to finish and return the total ops completed.
   */
  public int awaitCompletion() {
    try {
      completeLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    acceptingWork = false;
    stopAdjuster();

    for (Thread t : threads.keySet()) {
      try {
        t.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    long opsDone = getOpsDone();
    return opsDone > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) opsDone;
  }

  public void stopAdjuster() {
    if (adjusterThread != null) {
      adjusterThread.interrupt();
      try {
        adjusterThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void runAdjuster() {
    final Measurements measurements = Measurements.getMeasurements();
    // Discard latency accumulated during startup/ramp so the first adjustment uses a clean window.
    measurements.getAndResetWindowAverageLatencyUs();
    while (acceptingWork && completeLatch.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(adjustIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      if (!acceptingWork || completeLatch.getCount() == 0) {
        break;
      }
      double windowLatencyUs = measurements.getAndResetWindowAverageLatencyUs();
      if (windowLatencyUs <= 0) {
        sampleClientThreads();
        continue;
      }
      int newLimit = computeNewLimit(currentLimit, minLatencyUs, windowLatencyUs, minThreads, maxThreads);
      if (newLimit != currentLimit) {
        logger.info("Thread count now {} - latency at {} ms",
            newLimit, String.format("%.2f", windowLatencyUs / 1000.0));
        setLimit(newLimit);
      } else {
        sampleClientThreads();
      }
    }
  }

  private void runConcurrencyAdjuster() {
    final Measurements measurements = Measurements.getMeasurements();
    measurements.getAndResetWindowAverageLatencyUs();
    while (acceptingWork && completeLatch.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
      try {
        Thread.sleep(adjustIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      if (!acceptingWork || completeLatch.getCount() == 0) {
        break;
      }
      double windowLatencyUs = measurements.getAndResetWindowAverageLatencyUs();
      if (windowLatencyUs <= 0) {
        sampleClientConcurrency();
        continue;
      }
      int newLimit = computeNewLimit(currentConcurrency, minLatencyUs, windowLatencyUs,
          minConcurrency, maxConcurrency);
      if (newLimit != currentConcurrency) {
        logger.info("Concurrency now {} - latency at {} ms",
            newLimit, String.format("%.2f", windowLatencyUs / 1000.0));
        setConcurrencyLimit(newLimit);
      } else {
        sampleClientConcurrency();
      }
    }
  }

  private synchronized void setConcurrencyLimit(int newLimit) {
    newLimit = clamp(newLimit, minConcurrency, maxConcurrency);
    currentConcurrency = newLimit;
    concurrencyLimiter.setLimit(newLimit);
    sampleClientConcurrency();
  }

  private synchronized void setLimit(int newLimit) {
    newLimit = clamp(newLimit, minThreads, maxThreads);
    currentLimit = newLimit;
    updateTargets();

    int running = activeWorkers.get();
    if (newLimit > running) {
      try {
        spawnWorkers(newLimit - running);
      } catch (UnknownDBException e) {
        logger.error("Failed to spawn additional client threads", e);
      }
    } else if (newLimit < running) {
      int toStop = running - newLimit;
      synchronized (clients) {
        for (int i = clients.size() - 1; i >= 0 && toStop > 0; i--) {
          ClientThread client = clients.get(i);
          if (!client.isThreadStopRequested() && client.requestThreadStop()) {
            toStop--;
          }
        }
      }
    }
    sampleClientThreads();
  }

  private void spawnFixedWorkers() throws UnknownDBException {
    double targetPerThreadMs = targetPerThreadMs(currentLimit);
    int threadcount = currentLimit;
    for (int threadid = 0; threadid < threadcount; threadid++) {
      int threadopcount = totalOpcount == 0 ? 0 : totalOpcount / threadcount;
      if (totalOpcount > 0 && threadid < totalOpcount % threadcount) {
        ++threadopcount;
      }
      spawnOne(threadopcount, targetPerThreadMs, threadcount);
    }
  }

  private void spawnFixedReactiveWorkers() throws UnknownDBException {
    double targetPerThreadMs = targetPerThreadMs(currentLimit);
    int threadcount = currentLimit;
    for (int threadid = 0; threadid < threadcount; threadid++) {
      int threadopcount = totalOpcount == 0 ? 0 : totalOpcount / threadcount;
      if (totalOpcount > 0 && threadid < totalOpcount % threadcount) {
        ++threadopcount;
      }
      spawnOneReactive(threadopcount, targetPerThreadMs, threadcount);
    }
  }

  private void spawnWorkers(int count) throws UnknownDBException {
    if (count <= 0) {
      return;
    }
    double targetPerThreadMs = targetPerThreadMs(currentLimit);
    for (int i = 0; i < count; i++) {
      // Variable mode uses a shared op budget (or unlimited); per-thread opcount is 0.
      spawnOne(0, targetPerThreadMs, threadcountForWorkload);
    }
  }

  private void spawnOne(int threadOpcount, double targetPerThreadMs, int workloadThreadcount)
      throws UnknownDBException {
    DB db = DBFactory.newDB(dbname, props, tracer);
    ClientThread client = new ClientThread(db, dotransactions, workload, props, threadOpcount,
        targetPerThreadMs, null, sharedOpsRemaining, this::onWorkerFinished);
    int threadId = nextThreadId.getAndIncrement();
    client.setThreadId(threadId);
    client.setThreadCount(workloadThreadcount);
    clients.add(client);

    activeWorkers.incrementAndGet();
    Thread t = new Thread(tracer.wrap(client, "ClientThread"), "ClientThread-" + threadId);
    threads.put(t, client);
    t.start();
  }

  private void spawnOneReactive(int threadOpcount, double targetPerThreadMs, int workloadThreadcount)
      throws UnknownDBException {
    DBReactive db = DBFactory.newDBReactive(dbname, props, tracer);
    ReactiveClientThread client = new ReactiveClientThread(db, dotransactions, workload, props, threadOpcount,
        targetPerThreadMs, concurrencyLimiter, sharedOpsRemaining, this::onWorkerFinished);
    int threadId = nextThreadId.getAndIncrement();
    client.setThreadId(threadId);
    client.setThreadCount(workloadThreadcount);
    reactiveClients.add(client);

    activeWorkers.incrementAndGet();
    Thread t = new Thread(tracer.wrap(client, "ReactiveClientThread"), "ReactiveClientThread-" + threadId);
    threads.put(t, client);
    t.start();
  }

  private double targetPerThreadMs(int threadLimit) {
    if (targetOpsPerSec <= 0 || threadLimit <= 0) {
      return -1;
    }
    return ((double) targetOpsPerSec) / ((double) threadLimit) / 1000.0;
  }

  private void updateTargets() {
    double targetPerThreadMs = targetPerThreadMs(currentLimit);
    synchronized (clients) {
      for (ClientThread client : clients) {
        if (!client.isThreadStopRequested()) {
          client.setTargetOpsPerThreadMs(targetPerThreadMs);
        }
      }
    }
  }

  /**
   * Sample the configured client-thread limit (not JVM threads). Using the limit avoids
   * end-of-run races where {@code activeWorkers} has already dropped to 0.
   */
  private synchronized void sampleClientThreads() {
    int n = currentLimit;
    if (n < minClientThreads) {
      minClientThreads = n;
    }
    if (n > maxClientThreads) {
      maxClientThreads = n;
    }
    threadSampleSum += n;
    threadSampleCount++;
  }

  private synchronized void sampleClientConcurrency() {
    int n = currentConcurrency;
    if (n < minClientConcurrency) {
      minClientConcurrency = n;
    }
    if (n > maxClientConcurrency) {
      maxClientConcurrency = n;
    }
    concurrencySampleSum += n;
    concurrencySampleCount++;
  }

  private void onWorkerFinished() {
    int left = activeWorkers.decrementAndGet();
    if (variableMode && acceptingWork && !workload.isStopRequested() && hasRemainingWork()) {
      synchronized (this) {
        int deficit = currentLimit - activeWorkers.get();
        if (deficit > 0 && acceptingWork && completeLatch.getCount() > 0) {
          try {
            spawnWorkers(deficit);
            return;
          } catch (UnknownDBException e) {
            logger.error("Failed to respawn client threads", e);
          }
        }
      }
    }
    if (left == 0) {
      acceptingWork = false;
      completeLatch.countDown();
    }
  }

  private boolean hasRemainingWork() {
    if (workload.isStopRequested()) {
      return false;
    }
    if (sharedOpsRemaining != null) {
      return sharedOpsRemaining.get() > 0;
    }
    // Unlimited operation count (opcount == 0) still has work until stop/time limit.
    return totalOpcount == 0;
  }

  static int clamp(int value, int min, int max) {
    return Math.max(min, Math.min(max, value));
  }

  /**
   * Compute the next thread/concurrency limit from the latency gradient controller.
   */
  static int computeNewLimit(int currentLimit, double minLatencyUs, double currentLatencyUs,
                             int minThreads, int maxThreads) {
    if (currentLatencyUs <= 0) {
      return currentLimit;
    }
    double gradient = minLatencyUs / currentLatencyUs;
    int newLimit = (int) Math.round(currentLimit * gradient);
    return clamp(newLimit, minThreads, maxThreads);
  }
}
