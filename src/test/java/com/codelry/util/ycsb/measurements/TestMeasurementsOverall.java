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
package com.codelry.util.ycsb.measurements;

import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

public class TestMeasurementsOverall {
  @Test
  public void testWarmupExcludedFromOverallAverage() {
    Properties props = new Properties();
    props.setProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "histogram");
    Measurements mm = new Measurements(props);

    mm.measure("WARMUP", 5000, false);
    mm.measure("READ", 1000, true);
    mm.measure("READ", 3000, true);

    assertEquals(2000.0, mm.getAverageLatencyUs(), 0.001);
  }

  @Test
  public void testWindowAverageResets() {
    Properties props = new Properties();
    props.setProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "histogram");
    Measurements mm = new Measurements(props);

    mm.measure("READ", 1000);
    mm.measure("READ", 3000);
    assertEquals(2000.0, mm.getAndResetWindowAverageLatencyUs(), 0.001);
    assertEquals(0.0, mm.getAndResetWindowAverageLatencyUs(), 0.001);

    mm.measure("READ", 4000);
    assertEquals(4000.0, mm.getAndResetWindowAverageLatencyUs(), 0.001);
    // Cumulative overall is unchanged by window resets.
    assertEquals(8000.0 / 3.0, mm.getAverageLatencyUs(), 0.001);
  }
}
