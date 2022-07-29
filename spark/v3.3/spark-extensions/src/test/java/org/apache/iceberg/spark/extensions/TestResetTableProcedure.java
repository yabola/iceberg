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
package org.apache.iceberg.spark.extensions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestResetTableProcedure extends SparkExtensionsTestBase {

  private final String targetName;

  public TestResetTableProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    targetName = tableName("reset_table");
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", targetName);
  }

  @Test
  public void testResetTable() throws NoSuchTableException, ParseException {
    Assume.assumeTrue(
        "Register/Reset only implemented on Hive Catalogs",
        spark.conf().get("spark.sql.catalog." + catalogName + ".type").equals("hive"));

    long numRows1 = 100;
    long numRows2 = 200;
    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);

    spark
        .range(0, numRows1)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String metaLocation =
        ((HiveTableOperations) (((HasTableOperations) table).operations()))
            .currentMetadataLocation();
    spark
        .range(0, numRows2)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();
    table = Spark3Util.loadIcebergTable(spark, tableName);
    String newMetalocation =
        ((HiveTableOperations) (((HasTableOperations) table).operations()))
            .currentMetadataLocation();

    sql("CALL %s.system.register_table('%s', '%s')", catalogName, targetName, metaLocation);
    List<Object[]> oldResults = sql("SELECT * FROM %s", targetName);
    sql("CALL %s.system.reset_table('%s', '%s')", catalogName, targetName, newMetalocation);
    List<Object[]> newResults = sql("SELECT * FROM %s", targetName);

    Assert.assertEquals(
        "Should have the right row count in the procedure result", numRows1, oldResults.size());
    Assert.assertEquals(
        "Should have the right row count in the procedure result",
        numRows1 + numRows2,
        newResults.size());
    Assert.assertThrows(
        "Can't reset a nonexistent table.",
        org.apache.iceberg.exceptions.NoSuchTableException.class,
        () ->
            sql(
                "CALL %s.system.reset_table('%s', '%s')",
                catalogName, "nonExistTableName", newMetalocation));
  }
}
