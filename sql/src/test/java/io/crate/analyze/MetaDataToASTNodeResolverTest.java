/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Test;

public class MetaDataToASTNodeResolverTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testBuildCreateTableColumns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.test (" +
                      " bools boolean," +
                      " bytes byte," +
                      " strings string," +
                      " shorts short," +
                      " floats float," +
                      " doubles double," +
                      " ints integer," +
                      " longs long," +
                      " timestamp timestamp," +
                      " ip_addr ip," +
                      " arr_simple array(string)," +
                      " arr_geo_point array(geo_point)," +
                      " arr_obj array(object(strict) as (" +
                      "  col_1 long," +
                      "  col_2 string" +
                      " ))," +
                      " obj object as (" +
                      "  col_1 long," +
                      "  col_2 string" +
                      " )" +
                      ") " +
                      "clustered into 5 shards " +
                      "with (number_of_replicas = '0-all')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("doc.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                     "   \"arr_geo_point\" ARRAY(GEO_POINT),\n" +
                     "   \"arr_obj\" ARRAY(OBJECT (STRICT) AS (\n" +
                     "      \"col_1\" LONG,\n" +
                     "      \"col_2\" STRING\n" +
                     "   )),\n" +
                     "   \"arr_simple\" ARRAY(STRING),\n" +
                     "   \"bools\" BOOLEAN,\n" +
                     "   \"bytes\" BYTE,\n" +
                     "   \"doubles\" DOUBLE,\n" +
                     "   \"floats\" FLOAT,\n" +
                     "   \"ints\" INTEGER,\n" +
                     "   \"ip_addr\" IP,\n" +
                     "   \"longs\" LONG,\n" +
                     "   \"obj\" OBJECT (DYNAMIC) AS (\n" +
                     "      \"col_1\" LONG,\n" +
                     "      \"col_2\" STRING\n" +
                     "   ),\n" +
                     "   \"shorts\" SHORT,\n" +
                     "   \"strings\" STRING,\n" +
                     "   \"timestamp\" TIMESTAMP\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'dynamic',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTablePrimaryKey() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table myschema.test (" +
                      " pk_col_one long," +
                      " pk_col_two long," +
                      " primary key (pk_col_one, pk_col_two)" +
                      ") " +
                      "clustered into 5 shards " +
                      "with (number_of_replicas = '0-all', column_policy = 'strict')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"pk_col_one\" LONG,\n" +
                     "   \"pk_col_two\" LONG,\n" +
                     "   PRIMARY KEY (\"pk_col_one\", \"pk_col_two\")\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'strict',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableNotNull() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table myschema.test (" +
                      " col_a string," +
                      " col_b string not null index using fulltext," +
                      " primary key (col_a)" +
                      ") " +
                      "clustered into 5 shards " +
                      "with (number_of_replicas = '0-all', column_policy = 'strict')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"col_a\" STRING,\n" +
                     "   \"col_b\" STRING NOT NULL INDEX USING FULLTEXT WITH (\n" +
                     "      analyzer = 'standard'\n" +
                     "   ),\n" +
                     "   PRIMARY KEY (\"col_a\")\n" +
                     ")\n" +
                     "CLUSTERED BY (\"col_a\") INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'strict',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableClusteredByPartitionedBy() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("create table myschema.test (" +
                      " id long," +
                      " partition_column string," +
                      " cluster_column string" +
                      ") " +
                      "partitioned by (partition_column) " +
                      "clustered by (cluster_column) into 5 shards " +
                      "with (number_of_replicas = '0-all')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"cluster_column\" STRING,\n" +
                     "   \"id\" LONG,\n" +
                     "   \"partition_column\" STRING\n" +
                     ")\n" +
                     "CLUSTERED BY (\"cluster_column\") INTO 5 SHARDS\n" +
                     "PARTITIONED BY (\"partition_column\")\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'dynamic',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }


    @Test
    public void testBuildCreateTableIndexes() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table myschema.test (" +
                      " id long," +
                      " col_a string," +
                      " col_b string index using fulltext," +
                      " col_c string index off," +
                      " col_d object as (" +
                      "  a string" +
                      " )," +
                      " index col_a_col_b_ft using fulltext (col_a, col_b) with (" +
                      "  analyzer= 'english'" +
                      " )," +
                      " index col_d_a_ft using fulltext (col_d['a']) with (" +
                      "  analyzer= 'custom_analyzer'" +
                      " )," +
                      " index col_a_col_b_plain using plain (col_a, col_b)" +
                      ") " +
                      "clustered into 5 shards " +
                      "with (number_of_replicas = '0-all')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"col_a\" STRING,\n" +
                     "   \"col_b\" STRING INDEX USING FULLTEXT WITH (\n" +
                     "      analyzer = 'standard'\n" +
                     "   ),\n" +
                     "   \"col_c\" STRING INDEX OFF,\n" +
                     "   \"col_d\" OBJECT (DYNAMIC) AS (\n" +
                     "      \"a\" STRING\n" +
                     "   ),\n" +
                     "   \"id\" LONG,\n" +
                     "   INDEX \"col_a_col_b_ft\" USING FULLTEXT (\"col_b\", \"col_a\") WITH (\n" +
                     "      analyzer = 'english'\n" +
                     "   ),\n" +
                     "   INDEX \"col_d_a_ft\" USING FULLTEXT (\"col_d\"['a']) WITH (\n" +
                     "      analyzer = 'custom_analyzer'\n" +
                     "   ),\n" +
                     "   INDEX \"col_a_col_b_plain\" USING FULLTEXT (\"col_b\", \"col_a\") WITH (\n" +
                     "      analyzer = 'keyword'\n" +
                     "   )\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'dynamic',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }

    @Test
    public void testBuildCreateTableStorageDefinitions() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table myschema.test (" +
                      " s string storage with (columnstore =false)" +
                      ") " +
                      "clustered into 5 shards " +
                      "with (number_of_replicas = '0-all', column_policy = 'strict')")
            .build();
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        CreateTable node = MetaDataToASTNodeResolver.resolveCreateTable(tableInfo);
        assertEquals("CREATE TABLE IF NOT EXISTS \"myschema\".\"test\" (\n" +
                     "   \"s\" STRING STORAGE WITH (\n" +
                     "      columnstore = false\n" +
                     "   )\n" +
                     ")\n" +
                     "CLUSTERED INTO 5 SHARDS\n" +
                     "WITH (\n" +
                     "   \"allocation.max_retries\" = 5,\n" +
                     "   \"blocks.metadata\" = false,\n" +
                     "   \"blocks.read\" = false,\n" +
                     "   \"blocks.read_only\" = false,\n" +
                     "   \"blocks.read_only_allow_delete\" = false,\n" +
                     "   \"blocks.write\" = false,\n" +
                     "   column_policy = 'strict',\n" +
                     "   \"mapping.total_fields.limit\" = 1000,\n" +
                     "   max_ngram_diff = 1,\n" +
                     "   max_shingle_diff = 3,\n" +
                     "   number_of_replicas = '0-all',\n" +
                     "   refresh_interval = 1000,\n" +
                     "   \"routing.allocation.enable\" = 'all',\n" +
                     "   \"routing.allocation.total_shards_per_node\" = -1,\n" +
                     "   \"translog.durability\" = 'REQUEST',\n" +
                     "   \"translog.flush_threshold_size\" = 536870912,\n" +
                     "   \"translog.sync_interval\" = 5000,\n" +
                     "   \"unassigned.node_left.delayed_timeout\" = 60000,\n" +
                     "   \"warmer.enabled\" = true,\n" +
                     "   \"write.wait_for_active_shards\" = 'ALL'\n" +
                     ")",
            SqlFormatter.formatSql(node));
    }
}
