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

/**
 * Limits the number of concurrent in-flight operations. When the limit is lowered below
 * the current in-flight count, new acquires block until enough operations complete.
 */
public final class ConcurrencyLimiter {
  private final Object lock = new Object();
  private int limit;
  private int inFlight;

  public ConcurrencyLimiter(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("concurrency limit must be >= 1");
    }
    this.limit = limit;
  }

  public void acquire() throws InterruptedException {
    synchronized (lock) {
      while (inFlight >= limit) {
        lock.wait();
      }
      inFlight++;
    }
  }

  public void release() {
    synchronized (lock) {
      if (inFlight > 0) {
        inFlight--;
      }
      lock.notifyAll();
    }
  }

  public void setLimit(int newLimit) {
    if (newLimit < 1) {
      throw new IllegalArgumentException("concurrency limit must be >= 1");
    }
    synchronized (lock) {
      this.limit = newLimit;
      lock.notifyAll();
    }
  }

  public int getLimit() {
    synchronized (lock) {
      return limit;
    }
  }

  public int getInFlight() {
    synchronized (lock) {
      return inFlight;
    }
  }
}
