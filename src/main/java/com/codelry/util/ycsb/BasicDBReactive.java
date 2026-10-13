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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Reactive counterpart to {@link BasicDB}: completes operations asynchronously (optionally delayed)
 * without talking to a real database. Useful for exercising the reactive client path.
 */
public class BasicDBReactive extends DBReactive {
  private static final Logger logger = LoggerFactory.getLogger(BasicDBReactive.class);

  public static final String VERBOSE = "basicdb.verbose";
  public static final String VERBOSE_DEFAULT = "false";

  public static final String SIMULATE_DELAY = "basicdb.simulatedelay";
  public static final String SIMULATE_DELAY_DEFAULT = "0";

  public static final String RANDOMIZE_DELAY = "basicdb.randomizedelay";
  public static final String RANDOMIZE_DELAY_DEFAULT = "true";

  private static final ScheduledExecutorService DELAY_EXECUTOR =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "BasicDBReactive-delay");
        t.setDaemon(true);
        return t;
      });

  private boolean verbose;
  private boolean randomizedelay;
  private int todelay;

  @Override
  public void init() {
    verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
    todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
    randomizedelay = Boolean.parseBoolean(getProperties().getProperty(RANDOMIZE_DELAY, RANDOMIZE_DELAY_DEFAULT));
    if (verbose) {
      Properties p = getProperties();
      logger.debug("BasicDBReactive properties: {}", p);
    }
  }

  private CompletableFuture<Status> completeOk() {
    if (todelay <= 0) {
      return CompletableFuture.completedFuture(Status.OK);
    }
    long delayMs;
    if (randomizedelay) {
      delayMs = ThreadLocalRandom.current().nextInt(Math.max(1, todelay + 1));
    } else {
      delayMs = todelay;
    }
    final CompletableFuture<Status> future = new CompletableFuture<>();
    DELAY_EXECUTOR.schedule(() -> future.complete(Status.OK), delayMs, TimeUnit.MILLISECONDS);
    return future;
  }

  @Override
  public CompletableFuture<Status> read(String table, String key, Set<String> fields,
                                        Map<String, ByteIterator> result) {
    if (verbose) {
      logger.debug("READ {} {}", table, key);
    }
    return completeOk();
  }

  @Override
  public CompletableFuture<Status> scan(String table, String startkey, int recordcount, Set<String> fields,
                                        Vector<HashMap<String, ByteIterator>> result) {
    if (verbose) {
      logger.debug("SCAN {} {} {}", table, startkey, recordcount);
    }
    return completeOk();
  }

  @Override
  public CompletableFuture<Status> update(String table, String key, Map<String, ByteIterator> values) {
    if (verbose) {
      logger.debug("UPDATE {} {}", table, key);
    }
    return completeOk();
  }

  @Override
  public CompletableFuture<Status> insert(String table, String key, Map<String, ByteIterator> values) {
    if (verbose) {
      logger.debug("INSERT {} {}", table, key);
    }
    return completeOk();
  }

  @Override
  public CompletableFuture<Status> delete(String table, String key) {
    if (verbose) {
      logger.debug("DELETE {} {}", table, key);
    }
    return completeOk();
  }
}
