<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
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
other YCSB tests.

- Maven coordinates: `com.codelry.util.ycsb:ycsb-core`
- Repository: https://github.com/mminichino/ycsb-core
- Based on the [Yahoo! Cloud Serving Benchmark](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

## Using as a dependency

### Gradle

```groovy
dependencies {
    implementation 'com.codelry.util.ycsb:ycsb-core:0.18.5'
}
```

### Maven

```xml
<dependency>
  <groupId>com.codelry.util.ycsb</groupId>
  <artifactId>ycsb-core</artifactId>
  <version>0.18.5</version>
</dependency>
```

## Implementing a client binding

Create a class that extends `com.codelry.util.ycsb.DB` and implement `read`, `scan`,
`update`, `insert`, and `delete`. Configure the binding with the `db` property
(for example in `db.properties` or `ycsb.properties` on the classpath):

```properties
db=com.example.MyDB
```

Optional properties such as `readandinsert` are available on the shared `Properties`
object passed to the binding via `setProperties` / `getProperties()`.

## Entrypoint

The application entrypoint is `com.codelry.util.ycsb.RunBenchmark`, which drives
`com.codelry.util.ycsb.Benchmark`.

Each workload run performs this cycle automatically:

1. **Setup** (if `test.setup` is defined)
2. **Load** (bulk insert)
3. **Run** (transaction phase)
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
    implementation 'com.codelry.util.ycsb:ycsb-core:0.18.5'
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

Java 11 toolchain; bytecode targets Java 8 (`options.release = 8`).

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

## Links

* [Original YCSB project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [Original Yahoo! announcement](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)
