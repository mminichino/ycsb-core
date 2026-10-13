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

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Client worker that submits operations asynchronously against a {@link DBReactive},
 * gating submits with a shared {@link ConcurrencyLimiter}.
 */
public class ReactiveClientThread implements Runnable {
  private static boolean spinSleep;

  private final DBReactive db;
  private final boolean dotransactions;
  private final Workload workload;
  private final int opcount;
  private final Properties props;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final AtomicLong sharedOpsRemaining;
  private final Runnable onComplete;
  private final Measurements measurements;

  private double targetOpsPerMs;
  private long targetOpsTickNs;
  private int threadid;
  private int threadcount;
  private Object workloadstate;

  private final AtomicInteger opsDone = new AtomicInteger(0);
  private final AtomicInteger opsSubmitted = new AtomicInteger(0);
  private final AtomicInteger pending = new AtomicInteger(0);
  private final AtomicBoolean stopThread = new AtomicBoolean(false);

  public ReactiveClientThread(DBReactive db, boolean dotransactions, Workload workload, Properties props,
                              int opcount, double targetperthreadperms, ConcurrencyLimiter concurrencyLimiter,
                              AtomicLong sharedOpsRemaining, Runnable onComplete) {
    this.db = db;
    this.dotransactions = dotransactions;
    this.workload = workload;
    this.opcount = opcount;
    this.props = props;
    this.concurrencyLimiter = concurrencyLimiter;
    this.sharedOpsRemaining = sharedOpsRemaining;
    this.onComplete = onComplete;
    this.measurements = Measurements.getMeasurements();
    spinSleep = Boolean.valueOf(this.props.getProperty("spin.sleep", "false"));
    setTargetOpsPerThreadMs(targetperthreadperms);
  }

  public void setThreadId(final int threadId) {
    threadid = threadId;
  }

  public void setThreadCount(final int threadCount) {
    threadcount = threadCount;
  }

  public int getOpsDone() {
    return opsDone.get();
  }

  public boolean requestThreadStop() {
    return !stopThread.getAndSet(true);
  }

  public boolean isThreadStopRequested() {
    return stopThread.get();
  }

  public synchronized void setTargetOpsPerThreadMs(double targetperthreadperms) {
    if (targetperthreadperms > 0) {
      targetOpsPerMs = targetperthreadperms;
      targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
    } else {
      targetOpsPerMs = 0;
      targetOpsTickNs = 0;
    }
  }

  int getOpsTodo() {
    if (sharedOpsRemaining != null) {
      return 0;
    }
    int todo = opcount - opsSubmitted.get();
    return todo < 0 ? 0 : todo;
  }

  @Override
  public void run() {
    try {
      db.init();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      finish();
      return;
    }

    try {
      workloadstate = workload.initThread(props, threadid, threadcount);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      finish();
      return;
    }

    if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
      long randomMinorDelay = ThreadLocalRandom.current().nextInt((int) targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }

    try {
      long startTimeNanos = System.nanoTime();
      while (!workload.isStopRequested() && !stopThread.get()) {
        if (!reserveNextOp()) {
          break;
        }

        try {
          concurrencyLimiter.acquire();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }

        if (workload.isStopRequested() || stopThread.get()) {
          concurrencyLimiter.release();
          break;
        }

        CompletableFuture<Boolean> future;
        try {
          if (dotransactions) {
            future = workload.doTransactionAsync(db, workloadstate);
          } else {
            future = workload.doInsertAsync(db, workloadstate);
          }
        } catch (RuntimeException e) {
          concurrencyLimiter.release();
          e.printStackTrace();
          e.printStackTrace(System.out);
          break;
        }

        if (future == null) {
          concurrencyLimiter.release();
          break;
        }

        pending.incrementAndGet();
        opsSubmitted.incrementAndGet();

        future.whenComplete((ok, err) -> {
          try {
            if (err != null || Boolean.FALSE.equals(ok)) {
              stopThread.set(true);
            }
            opsDone.incrementAndGet();
          } finally {
            concurrencyLimiter.release();
            pending.decrementAndGet();
          }
        });

        throttleNanos(startTimeNanos);
      }

      drainPending();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
    } finally {
      finish();
    }
  }

  private void finish() {
    if (onComplete != null) {
      onComplete.run();
    }
  }

  private void drainPending() {
    while (pending.get() > 0) {
      LockSupport.parkNanos(100_000L);
    }
  }

  private boolean reserveNextOp() {
    if (sharedOpsRemaining != null) {
      long left = sharedOpsRemaining.getAndDecrement();
      if (left <= 0) {
        sharedOpsRemaining.incrementAndGet();
        return false;
      }
      return true;
    }
    return (opcount == 0) || (opsSubmitted.get() < opcount);
  }

  private static void sleepUntil(long deadline) {
    while (System.nanoTime() < deadline) {
      if (!spinSleep) {
        LockSupport.parkNanos(deadline - System.nanoTime());
      }
    }
  }

  private void throttleNanos(long startTimeNanos) {
    double target;
    long tickNs;
    synchronized (this) {
      target = targetOpsPerMs;
      tickNs = targetOpsTickNs;
    }
    if (target > 0) {
      long deadline = startTimeNanos + opsSubmitted.get() * tickNs;
      sleepUntil(deadline);
      measurements.setIntendedStartTimeNs(deadline);
    }
  }
}
