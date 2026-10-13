<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# ycsb-core

YCSB-compatible core library for publishing **standalone** database benchmark projects
without merging client bindings into the main
[YCSB](https://github.com/brianfrankcooper/YCSB) repository.

Import this library as a dependency, implement your client binding in your own project,
and run the standard YCSB workloads. Results remain compatible and comparable with
other YCSB tests when using the standard synchronous `DB` interface.

- Maven coordinates: `com.codelry.util.ycsb:ycsb-core`
- Repository: https://github.com/mminichino/ycsb-core
- Based on the [Yahoo! Cloud Serving Benchmark](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

## Using as a dependency

### Gradle

```groovy
dependencies {
    implementation 'com.codelry.util.ycsb:ycsb-core:0.18.8'
}
```

### Maven

```xml
<dependency>
  <groupId>com.codelry.util.ycsb</groupId>
  <artifactId>ycsb-core</artifactId>
  <version>0.18.8</version>
</dependency>
```

## Implementing a client binding

### Synchronous (`DB`)

Create a class that extends `com.codelry.util.ycsb.DB` and implement `read`, `scan`,
`update`, `insert`, and `delete`. Configure the binding with the `db` property
(for example in `db.properties` or `ycsb.properties` on the classpath):

```properties
db=com.example.MyDB
```

Optional properties such as `readandinsert` are available on the shared `Properties`
object passed to the binding via `setProperties` / `getProperties()`.

### Reactive (`DBReactive`)

For databases with a reactive or future-based SDK, extend
`com.codelry.util.ycsb.DBReactive` and return `CompletableFuture<Status>` from each
operation. Selecting a `DBReactive` binding enables the reactive client path
automatically.

```java
public class MyReactiveDB extends DBReactive {
  @Override
  public CompletableFuture<Status> read(String table, String key, Set<String> fields,
                                        Map<String, ByteIterator> result) {
    return sdk.read(table, key, fields)
        .thenApply(rows -> { /* fill result */ return Status.OK; });
  }
  // scan / update / insert / delete similarly
}
```

```properties
db=com.example.MyReactiveDB
concurrency=256
```

Reactive runs use fixed worker threads (CPU count by default) and limit in-flight
operations with `concurrency`. Latency and OK/ERROR status are recorded when each
future completes. Call out reactive-path results when publishing numbers if they
are not intended to be compared with classic one-op-per-thread YCSB runs.

`CoreWorkload` implements the async workload methods required by this path. Other
workloads must override `doInsertAsync` / `doTransactionAsync` to be used with a
reactive binding.

## Client threads and concurrency

| Property | Applies to | Description |
|----------|------------|-------------|
| `threadcount` | Sync `DB` | Worker threads (default: `1`). Set to `0` for **variable thread count**. |
| `threadcount` | Reactive `DBReactive` | Fixed worker threads. Default: CPU count. `0` means CPU count (variable threads are not supported). |
| `threadcount.min` / `threadcount.max` | Sync variable threads | Bounds for variable thread count (defaults: CPU count / `256`). |
| `threadcount.adjustinterval` | Sync variable threads | Seconds between adjustments (default: `10`). |
| `threadcount.minlatency` | Sync variable threads | Target minimum latency in ms for the controller (default: `1`). |
| `concurrency` | Reactive | Max in-flight operations (default: `256`). Set to `0` for **variable concurrency**. |
| `concurrency.min` / `concurrency.max` | Reactive variable concurrency | Bounds (defaults: CPU count / `256`). |
| `concurrency.adjustinterval` | Reactive variable concurrency | Seconds between adjustments (default: `10`). |
| `concurrency.minlatency` | Reactive variable concurrency | Target minimum latency in ms (default: `1`). |

Variable modes use a latency-gradient controller:
`new_limit = current_limit * (min_latency / window_latency)`, clamped to min/max.
Status logs report the starting thread (and concurrency) settings at the beginning
of each load/transactions phase, and log changes as
`Thread count now X - latency at Y ms` or `Concurrency now X - latency at Y ms`.

OVERALL export includes `MinThreads` / `MaxThreads` / `AverageThreads`. Reactive
runs also export `MinConcurrency` / `MaxConcurrency` / `AverageConcurrency`.

## Warmup

Bindings may override `DB.warmup()` / `DBReactive.warmup()` for platform-specific
preparation before the timed transaction phase. Warmup runs once before the run
phase (not load). Metrics are recorded as `WARMUP` and are excluded from OVERALL
aggregates and run time.

## Entrypoint

The application entrypoint is `com.codelry.util.ycsb.RunBenchmark`, which drives
`com.codelry.util.ycsb.Benchmark`.

Each workload run performs this cycle automatically:

1. **Setup** (if `test.setup` is defined)
2. **Load** (bulk insert)
3. **Run** (transaction phase; optional warmup first)
4. **Cleanup** (if `test.clean` is defined)

Without `-w`, all standard workloads (`a`–`f`) are executed in sequence. With `-w`,
only the selected workload runs:

```text
# all workloads
bin/<your-app>

# single workload (for example Workload E)
bin/<your-app> -w e
```

## Example consumer project (Gradle)

```groovy
plugins {
    id 'java'
    id 'application'
}

dependencies {
    implementation 'com.codelry.util.ycsb:ycsb-core:0.18.8'
}

application {
    mainClass = 'com.codelry.util.ycsb.RunBenchmark'
    applicationDistribution.from("src/main/conf/") {
        into "conf"
    }
}
```

Put runtime configuration under `src/main/conf/` (or otherwise on the classpath),
typically including:

| File | Purpose |
|------|---------|
| `ycsb.properties` | Common run settings (`recordcount`, `operationcount`, `threadcount`, `db`, …) |
| `db.properties` | Database-specific settings for your binding |

Workload definitions (`workloada` … `workloadf`) ship with this library.

Build and run:

```sh
./gradlew installDist
build/install/<your-app>/bin/<your-app>
build/install/<your-app>/bin/<your-app> -w a
```

## Test setup and cleanup

Between workload runs you can hook prep and teardown classes:

| Property | Interface | When it runs |
|----------|-----------|--------------|
| `test.setup` | `com.codelry.util.ycsb.TestSetup` | Before the **load** phase |
| `test.clean` | `com.codelry.util.ycsb.TestCleanup` | After the **run** phase |

Example:

```properties
test.setup=com.example.MyTestSetup
test.clean=com.example.MyTestCleanup
```

```java
public class MyTestSetup extends TestSetup {
  @Override
  public void testSetup(Properties properties) {
    // create buckets, truncate tables, etc.
  }
}

public class MyTestCleanup extends TestCleanup {
  @Override
  public void testClean(Properties properties) {
    // drop temporary resources, flush state, etc.
  }
}
```

## Building this repository

```sh
./gradlew build
```

Java 17 toolchain; bytecode targets Java 8 (`options.release = 8`).

## Latency percentiles

Prefer reporting high percentiles (P99 and the tail: P99.9, P99.99, …) rather than
averages. Latency percentiles must not be averaged across loaders.

When running multiple loaders, dump HDR histograms and merge them offline:

```text
-p hdrhistogram.fileoutput=true
-p hdrhistogram.output.path=file.hdr
```

See [HdrLogProcessing](https://github.com/nitsanw/HdrLogProcessing) and
[HdrHistogram](https://github.com/HdrHistogram/HdrHistogram) for merge/export tooling.

## Changelog

### 0.18.8

- Optional **reactive** binding path: extend `DBReactive` and return
  `CompletableFuture<Status>`; wrapped by `DBWrapperReactive` (measure/status on
  completion).
- Reactive client loop with shared in-flight limit (`concurrency`, default 256).
- **Variable concurrency** when `concurrency=0` (same latency-gradient controller
  as variable threads). Fixed worker threads default to CPU count; variable
  thread count is not used on this path.
- `CoreWorkload` async transaction/insert methods; `BasicDBReactive` test binding.
- Phase-start logging for client threads / concurrency; adjustment logs in the
  form `Thread count now X - latency at Y ms` /
  `Concurrency now X - latency at Y ms`.
- OVERALL export of min/max/average concurrency for reactive runs.

### 0.18.7

- **Variable thread count** when `threadcount=0`, with
  `threadcount.min` / `threadcount.max` / `threadcount.adjustinterval` /
  `threadcount.minlatency`.
- Optional DB **warmup** before the timed transaction phase (`DB.warmup()`);
  recorded as `WARMUP` and excluded from OVERALL aggregates.
- `ClientThreadPool` manages workers and optional thread-count adjustment;
  OVERALL export of min/max/average client threads.

## Links

* [Original YCSB project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [Original Yahoo! announcement](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)
