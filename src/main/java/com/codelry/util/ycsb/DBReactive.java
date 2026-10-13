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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous database layer for bindings that use a reactive or future-based SDK.
 *
 * <p>Parallel to {@link DB}: each client worker thread receives its own instance.
 * Methods return {@link CompletableFuture}s that complete when the operation finishes.
 * Result maps/vectors must not be read until the returned future completes.
 *
 * <p>Selecting a class that extends {@code DBReactive} (via the {@code db} property)
 * enables the reactive client path: fixed worker threads defaulting to CPU count,
 * and a {@code concurrency} limit on in-flight operations (default 256; {@code 0}
 * enables variable concurrency).
 */
public abstract class DBReactive {
  private Properties properties = new Properties();

  public void setProperties(Properties p) {
    properties = p;
  }

  public Properties getProperties() {
    return properties;
  }

  /**
   * Initialize any state for this DB instance. Called once per instance
   * (one instance per client worker thread).
   */
  public void init() throws DBException {
  }

  /**
   * Cleanup any state for this DB instance. Called once per instance after
   * in-flight operations have drained.
   */
  public void cleanup() throws DBException {
  }

  /**
   * Optional database platform-specific warmup. Default is a no-op.
   */
  public void warmup() throws DBException {
  }

  public abstract CompletableFuture<Status> read(String table, String key, Set<String> fields,
                                                 Map<String, ByteIterator> result);

  public abstract CompletableFuture<Status> scan(String table, String startkey, int recordcount,
                                                 Set<String> fields,
                                                 Vector<HashMap<String, ByteIterator>> result);

  public abstract CompletableFuture<Status> update(String table, String key,
                                                   Map<String, ByteIterator> values);

  public abstract CompletableFuture<Status> insert(String table, String key,
                                                   Map<String, ByteIterator> values);

  public abstract CompletableFuture<Status> delete(String table, String key);
}
