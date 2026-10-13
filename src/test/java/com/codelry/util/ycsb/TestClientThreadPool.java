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

import static org.testng.AssertJUnit.assertEquals;

public class TestClientThreadPool {
  @Test
  public void testComputeNewLimitScalesDownWhenLatencyHigh() {
    // minLatency 1000us, current 2000us => gradient 0.5 => 32 -> 16
    assertEquals(16, ClientThreadPool.computeNewLimit(32, 1000.0, 2000.0, 1, 256));
  }

  @Test
  public void testComputeNewLimitScalesUpWhenLatencyLow() {
    // minLatency 1000us, current 500us => gradient 2.0 => 32 -> 64
    assertEquals(64, ClientThreadPool.computeNewLimit(32, 1000.0, 500.0, 1, 256));
  }

  @Test
  public void testComputeNewLimitClampsToMax() {
    assertEquals(256, ClientThreadPool.computeNewLimit(200, 1000.0, 100.0, 1, 256));
  }

  @Test
  public void testComputeNewLimitClampsToMin() {
    assertEquals(4, ClientThreadPool.computeNewLimit(8, 1000.0, 10000.0, 4, 256));
  }

  @Test
  public void testComputeNewLimitUnchangedWhenNoLatency() {
    assertEquals(32, ClientThreadPool.computeNewLimit(32, 1000.0, 0.0, 1, 256));
  }

  @Test
  public void testClamp() {
    assertEquals(10, ClientThreadPool.clamp(10, 1, 256));
    assertEquals(1, ClientThreadPool.clamp(0, 1, 256));
    assertEquals(256, ClientThreadPool.clamp(1000, 1, 256));
  }
}
