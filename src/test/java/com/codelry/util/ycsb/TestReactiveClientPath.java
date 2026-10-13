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
import com.codelry.util.ycsb.workloads.CoreWorkload;
import org.apache.htrace.core.Tracer;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TestReactiveClientPath {
  @Test
  public void testIsReactiveBinding() {
    assertTrue(DBFactory.isReactiveBinding(BasicDBReactive.class.getName()));
    assertFalse(DBFactory.isReactiveBinding(BasicDB.class.getName()));
  }

  @Test
  public void testReactivePoolRunsOps() throws Exception {
    Properties props = new Properties();
    props.setProperty(Client.DB_PROPERTY, BasicDBReactive.class.getName());
    props.setProperty(Client.THREAD_COUNT_PROPERTY, "2");
    props.setProperty(Client.CONCURRENCY_PROPERTY, "8");
    props.setProperty(Client.RECORD_COUNT_PROPERTY, "100");
    props.setProperty(Client.OPERATION_COUNT_PROPERTY, "50");
    props.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");
    props.setProperty(CoreWorkload.TABLENAME_PROPERTY, "usertable");
    props.setProperty(Client.WORKLOAD_PROPERTY, CoreWorkload.class.getName());
    props.setProperty("basicdb.verbose", "false");
    props.setProperty("basicdb.simulatedelay", "0");

    Measurements.setProperties(props);
    Measurements.reset();

    Workload workload = new CoreWorkload();
    workload.init(props);

    Tracer tracer = new Tracer.Builder("test").build();
    ClientThreadPool pool = new ClientThreadPool(BasicDBReactive.class.getName(), props, workload, tracer,
        2, 50, 0);

    assertTrue(pool.isReactiveMode());
    assertFalse(pool.isVariableMode());
    assertFalse(pool.isVariableConcurrencyMode());
    assertEquals(2, pool.getCurrentLimit());
    assertEquals(8, pool.getCurrentConcurrency());

    pool.start();
    int ops = pool.awaitCompletion();
    assertEquals(50, ops);
    workload.cleanup();
  }

  @Test
  public void testReactiveDefaultThreadsAreCpuCountWhenUnset() {
    Properties props = new Properties();
    props.setProperty(Client.DB_PROPERTY, BasicDBReactive.class.getName());
    props.setProperty(Client.CONCURRENCY_PROPERTY, "16");
    props.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");
    Measurements.setProperties(props);
    Measurements.reset();

    Workload workload = new CoreWorkload();
    Tracer tracer = new Tracer.Builder("test").build();
    int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
    // configuredThreadcount=1 simulates Client default when property is absent
    ClientThreadPool pool = new ClientThreadPool(BasicDBReactive.class.getName(), props, workload, tracer,
        1, 0, 0);

    assertTrue(pool.isReactiveMode());
    assertEquals(cores, pool.getCurrentLimit());
    assertEquals(16, pool.getCurrentConcurrency());
  }

  @Test
  public void testReactiveThreadcountZeroUsesCpuCount() {
    Properties props = new Properties();
    props.setProperty(Client.DB_PROPERTY, BasicDBReactive.class.getName());
    props.setProperty(Client.THREAD_COUNT_PROPERTY, "0");
    props.setProperty(Client.CONCURRENCY_PROPERTY, "32");
    props.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");
    Measurements.setProperties(props);
    Measurements.reset();

    Workload workload = new CoreWorkload();
    Tracer tracer = new Tracer.Builder("test").build();
    int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
    ClientThreadPool pool = new ClientThreadPool(BasicDBReactive.class.getName(), props, workload, tracer,
        0, 0, 0);

    assertTrue(pool.isReactiveMode());
    assertFalse(pool.isVariableMode());
    assertEquals(cores, pool.getCurrentLimit());
  }

  @Test
  public void testVariableConcurrencyMode() {
    Properties props = new Properties();
    props.setProperty(Client.DB_PROPERTY, BasicDBReactive.class.getName());
    props.setProperty(Client.THREAD_COUNT_PROPERTY, "2");
    props.setProperty(Client.CONCURRENCY_PROPERTY, "0");
    props.setProperty(Client.MIN_CONCURRENCY_PROPERTY, "4");
    props.setProperty(Client.MAX_CONCURRENCY_PROPERTY, "64");
    props.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");
    Measurements.setProperties(props);
    Measurements.reset();

    Workload workload = new CoreWorkload();
    Tracer tracer = new Tracer.Builder("test").build();
    ClientThreadPool pool = new ClientThreadPool(BasicDBReactive.class.getName(), props, workload, tracer,
        2, 0, 0);

    assertTrue(pool.isVariableConcurrencyMode());
    assertEquals(2, pool.getCurrentLimit());
    assertTrue(pool.getCurrentConcurrency() >= 4);
    assertTrue(pool.getCurrentConcurrency() <= 64);
  }
}
