/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import com.google.common.collect.Iterables;
import java.io.IOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestNoResidualFileScanTask extends TableTestBase {

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestNoResidualFileScanTask(int formatVersion) {
    super(formatVersion);
  }

  private static final DataFile FILE = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(2)
      .build();

  @Test
  public void testResidualIsIgnored() throws IOException {
    table.newAppend().appendFile(FILE).commit();

    TableScan scan = table.newScan()
        .option("complete-files", "true")
        .filter(Expressions.lessThan("id", 10));

    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          Assert.assertEquals(
              "Residual expression should be alwaysTrue",
              fileScanTask.residual(), Expressions.alwaysTrue());
        }
      }
    }
  }
}
