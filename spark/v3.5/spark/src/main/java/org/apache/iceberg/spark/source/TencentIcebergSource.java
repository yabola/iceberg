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
package org.apache.iceberg.spark.source;

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import com.tencentcloudapi.dlc.v20210125.DlcClient;
import com.tencentcloudapi.dlc.v20210125.models.DescribeTableRequest;
import com.tencentcloudapi.dlc.v20210125.models.DescribeTableResponse;
import com.tencentcloudapi.dlc.v20210125.models.Property;
import com.tencentcloudapi.dlc.v20210125.models.TableResponseInfo;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
* 腾讯定制版 IcebergSource，注册为 "tc_iceberg" 数据源。
 *
 * <p>继承自开源 IcebergSource，增加了以下功能：
 * <ul>
 *   <li>支持通过腾讯云 DLC API 获取表的 metadata_location，直接加载 Iceberg 表</li>
 *   <li>未配置 DLC 表时，按照开源实现的 path 访问方式加载（super.getTable）</li>
 * </ul>
 *
 * <p>使用方式：spark.read().format("tc_iceberg").load(table)
 */
public class TencentIcebergSource extends IcebergSource {

  // Spark 配置项：腾讯云 COSN SecretId
  private static final String COSN_SECRET_ID_CONF = "spark.hadoop.fs.cosn.userinfo.secretId";
  // Spark 配置项：腾讯云 COSN SecretKey
  private static final String COSN_SECRET_KEY_CONF = "spark.hadoop.fs.cosn.userinfo.secretKey";
  // Spark 配置项：DLC 地域
  private static final String DLC_REGION_CONF = "spark.dlc.region";
  // Spark 配置项：DLC 默认 Catalog（datasourceConnectionName）
  private static final String DLC_DEFAULT_CATALOG_CONF = "spark.dlc.defaultCatalog";

  @Override
  public String shortName() {
    return "tc_sharing_iceberg";
  }

  /**
   * 重写 inferSchema，当配置了 dlc-table 时通过 DLC API 加载 Iceberg 表并返回其 schema。
   *
* <p>解决 Databricks 环境下 CREATE TABLE ... USING tc_iceberg 时，
   * DeltaCatalog/HiveExternalCatalog 因 schema 为 null 导致 NPE 的问题。
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    String dlcTable = options.get(SparkReadOptions.DLC_TABLE);
    if (dlcTable != null && !dlcTable.isEmpty()) {
      Table table = loadTableDirectlyFromDLCTableName(dlcTable, options);
      return table.schema();
    }
    // 未配置 dlc-table 时，走父类默认逻辑（返回 null，由 Spark 自行推断）
    return super.inferSchema(options);
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    CaseInsensitiveStringMap ciOptions = new CaseInsensitiveStringMap(options);

    // 优先：如果配置了 DLC_TABLE，通过腾讯云 DLC API 获取 metadata_location 直接加载
    String dlcTable = ciOptions.get(SparkReadOptions.DLC_TABLE);
    if (dlcTable != null && !dlcTable.isEmpty()) {
      return loadTableDirectlyFromDLCTableName(dlcTable, ciOptions);
    }

    // 其次：按照开源实现的 path 访问方式，通过 Spark CatalogManager 加载
    return super.getTable(schema, partitioning, options);
  }

  /**
   * 通过腾讯云 DLC API 获取表的 metadata_location，然后直接加载 Iceberg 表。
   *
   * <p>完全绕过 Spark CatalogManager，避免 Databricks UC 拦截。
   *
   * @param dlcTable DLC 表标识（两段式 database.table 或三段式 catalog.database.table）
   * @param options 用户传入的选项
   * @return 加载后的 SparkTable
   */
  private Table loadTableDirectlyFromDLCTableName(
      String dlcTable, CaseInsensitiveStringMap options) {
    SparkSession spark = SparkSession.active();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();

    String metadataLocation = resolveMetadataLocationViaDlc(dlcTable, spark);
    Preconditions.checkArgument(
        metadataLocation != null,
        "Failed to resolve metadata_location for DLC table: %s",
        dlcTable);

    Long snapshotId = propertyAsLong(options, SparkReadOptions.SNAPSHOT_ID);
    Long asOfTimestamp = propertyAsLong(options, SparkReadOptions.AS_OF_TIMESTAMP);
    String branch = options.get(SparkReadOptions.BRANCH);
    String tag = options.get(SparkReadOptions.TAG);

    // 通过 metadata_location 直接加载（只读，不需要 catalog）
    HadoopFileIO fileIO = new HadoopFileIO(hadoopConf);
    StaticTableOperations ops = new StaticTableOperations(metadataLocation, fileIO);
    org.apache.iceberg.Table icebergTable = new BaseTable(ops, metadataLocation);

    // 根据 time travel 参数返回对应的 SparkTable
    if (asOfTimestamp != null) {
      long resolvedSnapshotId =
          org.apache.iceberg.util.SnapshotUtil.snapshotIdAsOfTime(icebergTable, asOfTimestamp);
      return new SparkTable(icebergTable, resolvedSnapshotId, true);
    } else if (snapshotId != null) {
      return new SparkTable(icebergTable, snapshotId, true);
    } else if (branch != null) {
      return new SparkTable(icebergTable, branch, true);
    } else if (tag != null) {
      org.apache.iceberg.Snapshot tagSnapshot = icebergTable.snapshot(tag);
      Preconditions.checkArgument(
          tagSnapshot != null, "Cannot find snapshot associated with tag name: %s", tag);
      return new SparkTable(icebergTable, tagSnapshot.snapshotId(), true);
    } else {
      return new SparkTable(icebergTable, true);
    }
  }

  /**
   * 通过腾讯云 DLC API 获取表的真实 metadata_location。
   *
   * <p>dlc-table 参数支持两段式和三段式：
   * <ul>
   *   <li>两段式 "database.table"：datasourceConnectionName 从 spark.dlc.defaultCatalog 读取</li>
   *   <li>三段式 "catalog.database.table"：第一段作为 datasourceConnectionName</li>
   * </ul>
   *
   * <p>secretId/secretKey 从 spark.hadoop.fs.cosn.userinfo.secretId/secretKey 读取，
   * region 从 spark.dlc.region 读取。
   *
   * @param dlcTable DLC 表标识（两段式或三段式）
   * @param spark 当前 SparkSession
   * @return 真实的 metadata_location 路径，如果获取失败则返回 null
   */
  private String resolveMetadataLocationViaDlc(String dlcTable, SparkSession spark) {
    String[] parts = dlcTable.split("\\.");
    String datasourceConnectionName;
    String databaseName;
    String tableName;

    if (parts.length == 2) {
      databaseName = parts[0];
      tableName = parts[1];
      Preconditions.checkArgument(
          spark.conf().contains(DLC_DEFAULT_CATALOG_CONF),
          "spark.dlc.defaultCatalog is required when dlc-table is in two-part format (database.table)");
      datasourceConnectionName = spark.conf().get(DLC_DEFAULT_CATALOG_CONF);
    } else if (parts.length == 3) {
      datasourceConnectionName = parts[0];
      databaseName = parts[1];
      tableName = parts[2];
    } else {
      throw new IllegalArgumentException(
          String.format(
              "dlc-table 格式不正确: '%s'，应为两段式 (database.table) 或三段式 (catalog.database.table)",
              dlcTable));
    }

    Preconditions.checkArgument(
        spark.conf().contains(COSN_SECRET_ID_CONF),
        "spark.hadoop.fs.cosn.userinfo.secretId is required when dlc-table is specified");
    Preconditions.checkArgument(
        spark.conf().contains(COSN_SECRET_KEY_CONF),
        "spark.hadoop.fs.cosn.userinfo.secretKey is required when dlc-table is specified");
    Preconditions.checkArgument(
        spark.conf().contains(DLC_REGION_CONF),
        "spark.dlc.region is required when dlc-table is specified");

    String secretId = spark.conf().get(COSN_SECRET_ID_CONF);
    String secretKey = spark.conf().get(COSN_SECRET_KEY_CONF);
    String region = spark.conf().get(DLC_REGION_CONF);

    try {
      Credential cred = new Credential(secretId, secretKey);
      HttpProfile httpProfile = new HttpProfile();
      httpProfile.setEndpoint("dlc.tencentcloudapi.com");

      ClientProfile clientProfile = new ClientProfile();
      clientProfile.setHttpProfile(httpProfile);

      DlcClient client = new DlcClient(cred, region, clientProfile);

      DescribeTableRequest req = new DescribeTableRequest();
      req.setTableName(tableName);
      req.setDatabaseName(databaseName);
      req.setDatasourceConnectionName(datasourceConnectionName);

      DescribeTableResponse resp = client.DescribeTable(req);
      TableResponseInfo tableInfo = resp.getTable();
      if (tableInfo != null && tableInfo.getProperties() != null) {
        for (Property prop : tableInfo.getProperties()) {
          if ("metadata_location".equals(prop.getKey())) {
            return prop.getValue();
          }
        }
      }

      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "通过 DLC API 获取表 %s.%s (connection=%s) 的 metadata_location 失败",
              databaseName, tableName, datasourceConnectionName),
          e);
    }
  }

  private static Long propertyAsLong(CaseInsensitiveStringMap options, String property) {
    String value = options.get(property);
    if (value != null) {
      return Long.parseLong(value);
    }

    return null;
  }
}
