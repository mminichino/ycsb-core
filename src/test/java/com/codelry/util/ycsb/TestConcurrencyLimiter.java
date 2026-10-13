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

import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TestConcurrencyLimiter {
  @Test
  public void testAcquireReleaseRespectsLimit() throws Exception {
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(2);
    limiter.acquire();
    limiter.acquire();
    assertEquals(2, limiter.getInFlight());

    final AtomicInteger acquired = new AtomicInteger(0);
    final CountDownLatch started = new CountDownLatch(1);
    Thread blocker = new Thread(() -> {
      try {
        started.countDown();
        limiter.acquire();
        acquired.incrementAndGet();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    blocker.start();
    assertTrue(started.await(1, TimeUnit.SECONDS));
    Thread.sleep(50);
    assertEquals(0, acquired.get());

    limiter.release();
    blocker.join(1000);
    assertEquals(1, acquired.get());
    assertEquals(2, limiter.getInFlight());

    limiter.release();
    limiter.release();
    assertEquals(0, limiter.getInFlight());
  }

  @Test
  public void testLoweringLimitBlocksUntilDrain() throws Exception {
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
    limiter.acquire();
    limiter.acquire();
    limiter.acquire();
    limiter.setLimit(1);
    assertEquals(1, limiter.getLimit());
    assertEquals(3, limiter.getInFlight());

    final AtomicInteger acquired = new AtomicInteger(0);
    Thread waiter = new Thread(() -> {
      try {
        limiter.acquire();
        acquired.incrementAndGet();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    waiter.start();
    Thread.sleep(50);
    assertEquals(0, acquired.get());

    limiter.release();
    limiter.release();
    // still 1 in-flight and limit 1 — waiter still blocked
    Thread.sleep(50);
    assertEquals(0, acquired.get());

    limiter.release();
    waiter.join(1000);
    assertEquals(1, acquired.get());
    limiter.release();
  }
}
