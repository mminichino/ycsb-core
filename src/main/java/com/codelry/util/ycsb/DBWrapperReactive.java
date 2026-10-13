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
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around a {@link DBReactive} that measures latencies and counts return codes
 * when each operation future completes.
 */
public class DBWrapperReactive extends DBReactive {
  private static final Logger logger = LoggerFactory.getLogger(DBWrapperReactive.class);

  private final DBReactive db;
  private final Measurements measurements;
  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;
  private Set<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";
  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private static final AtomicBoolean LOG_REPORT_CONFIG = new AtomicBoolean(false);

  private final String scopeStringCleanup;
  private final String scopeStringInit;
  private final String scopeStringWarmup;

  public DBWrapperReactive(final DBReactive db, final Tracer tracer) {
    this.db = db;
    measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    scopeStringCleanup = simple + "#cleanup";
    scopeStringInit = simple + "#init";
    scopeStringWarmup = simple + "#warmup";
  }

  @Override
  public void setProperties(Properties p) {
    db.setProperties(p);
  }

  @Override
  public Properties getProperties() {
    return db.getProperties();
  }

  @Override
  public void init() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringInit)) {
      db.init();

      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
          getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
              REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
              latencyTrackedErrorsProperty.split(",")));
        }
      }

      if (LOG_REPORT_CONFIG.compareAndSet(false, true)) {
        logger.info("DBWrapperReactive: report latency for each error is {} and specific error codes "
                + "to track for latency are: {}",
            this.reportLatencyForEachError, this.latencyTrackedErrors);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

  @Override
  public void warmup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringWarmup)) {
      long st = System.nanoTime();
      db.warmup();
      long en = System.nanoTime();
      int latencyUs = (int) ((en - st) / 1000);
      measurements.measure("WARMUP", latencyUs, false);
      measurements.reportStatus("WARMUP", Status.OK);
    }
  }

  /**
   * Cleanup the underlying DB without recording CLEANUP measurements (used after warmup).
   */
  void cleanupWithoutMeasurement() throws DBException {
    db.cleanup();
  }

  @Override
  public CompletableFuture<Status> read(String table, String key, Set<String> fields,
                                        Map<String, ByteIterator> result) {
    return measureAsync("READ", db.read(table, key, fields, result));
  }

  @Override
  public CompletableFuture<Status> scan(String table, String startkey, int recordcount,
                                        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return measureAsync("SCAN", db.scan(table, startkey, recordcount, fields, result));
  }

  @Override
  public CompletableFuture<Status> update(String table, String key, Map<String, ByteIterator> values) {
    return measureAsync("UPDATE", db.update(table, key, values));
  }

  @Override
  public CompletableFuture<Status> insert(String table, String key, Map<String, ByteIterator> values) {
    return measureAsync("INSERT", db.insert(table, key, values));
  }

  @Override
  public CompletableFuture<Status> delete(String table, String key) {
    return measureAsync("DELETE", db.delete(table, key));
  }

  /**
   * Capture intended/start times on the submitting thread; measure and report status when the
   * future completes (possibly on another thread). Exceptions are mapped to {@link Status#ERROR}
   * so the client loop can drain cleanly.
   */
  private CompletableFuture<Status> measureAsync(String op, CompletableFuture<Status> future) {
    final long ist = measurements.getIntendedStartTimeNs();
    final long st = System.nanoTime();
    return future.handle((status, error) -> {
      long en = System.nanoTime();
      Status res;
      if (error != null || status == null) {
        res = Status.ERROR;
      } else {
        res = status;
      }
      measure(op, res, ist, st, en);
      measurements.reportStatus(op, res);
      return res;
    });
  }

  private void measure(String op, Status result, long intendedStartTimeNanos,
                       long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError ||
          (result != null && this.latencyTrackedErrors.contains(result.getName()))) {
        measurementName = op + "-" + (result == null ? "NULL" : result.getName());
      } else {
        measurementName = op + "-FAILED";
      }
    }
    measurements.measure(measurementName,
        (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(measurementName,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }
}
