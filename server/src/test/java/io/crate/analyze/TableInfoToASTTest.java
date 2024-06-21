/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeoReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.SqlFormatter;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;

public class TableInfoToASTTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @Test
    public void testBuildCreateTableColumns() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table doc.test (
                  bools boolean,
                  bytes byte,
                  strings string,
                  shorts short,
                  floats float,
                  doubles double,
                  ints integer,
                  longs long,
                  timestamp timestamp with time zone,
                  ip_addr ip,
                  arr_simple array(string),
                  arr_geo_point array(geo_point),
                  arr_obj array(object(strict) as (
                    col_1 long,
                    col_2 string
                  )),
                  obj object as (
                    col_1 long,
                    col_2 string
                  )
                )
                clustered into 5 shards
                with (
                  number_of_replicas = '0-all',
                  "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("doc.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "doc"."test" (
               "bools" BOOLEAN,
               "bytes" BYTE,
               "strings" TEXT,
               "shorts" SMALLINT,
               "floats" REAL,
               "doubles" DOUBLE PRECISION,
               "ints" INTEGER,
               "longs" BIGINT,
               "timestamp" TIMESTAMP WITH TIME ZONE,
               "ip_addr" IP,
               "arr_simple" ARRAY(TEXT),
               "arr_geo_point" ARRAY(GEO_POINT),
               "arr_obj" ARRAY(OBJECT(STRICT) AS (
                  "col_1" BIGINT,
                  "col_2" TEXT
               )),
               "obj" OBJECT(DYNAMIC) AS (
                  "col_1" BIGINT,
                  "col_2" TEXT
               )
            )
            CLUSTERED INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTablePrimaryKey() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table myschema.test (
                  pk_col_one long,
                  pk_col_two long,
                  primary key (pk_col_one, pk_col_two)
                )
                clustered into 5 shards
                with (
                  number_of_replicas = '0-all',
                  column_policy = 'strict',
                  "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "myschema"."test" (
               "pk_col_one" BIGINT NOT NULL,
               "pk_col_two" BIGINT NOT NULL,
               PRIMARY KEY ("pk_col_one", "pk_col_two")
            )
            CLUSTERED INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTableNotNull() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table myschema.test (
                  col_a string,
                  col_b string not null index using fulltext,
                  constraint c_1 primary key (col_a)
                )
                clustered into 5 shards\s
                with (
                  number_of_replicas = '0-all',
                  column_policy = 'strict',
                  "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "myschema"."test" (
               "col_a" TEXT NOT NULL,
               "col_b" TEXT NOT NULL INDEX USING FULLTEXT WITH (
                  analyzer = 'standard'
               ),
               CONSTRAINT c_1 PRIMARY KEY ("col_a")
            )
            CLUSTERED BY ("col_a") INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTableCheckConstraints() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table doc.test (
                  floats float constraint test_floats_check check (floats != -1),
                  shorts short,
                  constraint test_shorts_check check (shorts >= 0)
                )
                clustered into 5 shards
                with (
                  number_of_replicas = '0-all'
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("doc.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "doc"."test" (
               "floats" REAL,
               "shorts" SMALLINT,
               CONSTRAINT test_floats_check CHECK("floats" <> - 1),
               CONSTRAINT test_shorts_check CHECK("shorts" >= 0)
            )
            CLUSTERED INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTableClusteredByPartitionedBy() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addPartitionedTable("""
                create table myschema.test (id long, partition_column string, cluster_column string)
                partitioned by (partition_column)
                clustered by (cluster_column) into 5 shards
                with (
                 number_of_replicas = '0-all', "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "myschema"."test" (
               "id" BIGINT,
               "partition_column" TEXT,
               "cluster_column" TEXT
            )
            CLUSTERED BY ("cluster_column") INTO 5 SHARDS
            PARTITIONED BY ("partition_column")
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }


    @Test
    public void testBuildCreateTableIndexes() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table myschema.test (
                 id long,
                 col_a string,
                 col_b string index using fulltext,
                 col_c string index off,
                 col_d object as (
                  a string
                ),
                 index col_a_col_b_ft using fulltext (col_a, col_b) with (
                   analyzer= 'english'),
                 index col_d_a_ft using fulltext (col_d['a']) with (
                  analyzer= 'custom_analyzer'),
                 index col_a_col_b_plain using plain (col_a, col_b)
                )
                clustered into 5 shards
                with (
                 number_of_replicas = '0-all',
                 "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "myschema"."test" (
               "id" BIGINT,
               "col_a" TEXT,
               "col_b" TEXT INDEX USING FULLTEXT WITH (
                  analyzer = 'standard'
               ),
               "col_c" TEXT INDEX OFF,
               "col_d" OBJECT(DYNAMIC) AS (
                  "a" TEXT
               ),
               INDEX "col_a_col_b_plain" USING FULLTEXT ("col_a", "col_b") WITH (
                  analyzer = 'keyword'
               ),
               INDEX "col_d_a_ft" USING FULLTEXT ("col_d"['a']) WITH (
                  analyzer = 'custom_analyzer'
               ),
               INDEX "col_a_col_b_ft" USING FULLTEXT ("col_a", "col_b") WITH (
                  analyzer = 'english'
               )
            )
            CLUSTERED INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTableStorageDefinitions() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table myschema.test (
                  s string storage with (columnstore =false)
                )
                clustered into 5 shards
                with (
                  number_of_replicas = '0-all',
                  column_policy = 'strict',
                  "merge.scheduler.max_thread_count" = 1
                )""");
        DocTableInfo tableInfo = e.resolveTableInfo("myschema.test");

        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "myschema"."test" (
               "s" TEXT STORAGE WITH (
                  columnstore = false
               )
            )
            CLUSTERED INTO 5 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-all',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void testBuildCreateTableColumnDefaultClause() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE test (" +
                      "   col1 TEXT," +
                      "   col2 INTEGER DEFAULT 1 + 1," +
                      "   col3 TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(3)," +
                      "   col4 TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP(3)" +
                      ") with (" +
                      " \"merge.scheduler.max_thread_count\" = 1" +
                      ")");
        DocTableInfo tableInfo = e.resolveTableInfo("test");
        var node = new TableInfoToAST(tableInfo).toStatement();
        assertThat(SqlFormatter.formatSql(node)).isEqualTo("""
            CREATE TABLE IF NOT EXISTS "doc"."test" (
               "col1" TEXT,
               "col2" INTEGER DEFAULT 2,
               "col3" TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp(3),
               "col4" TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp(3)
            )
            CLUSTERED INTO 4 SHARDS
            WITH (
               "allocation.max_retries" = 5,
               "blocks.metadata" = false,
               "blocks.read" = false,
               "blocks.read_only" = false,
               "blocks.read_only_allow_delete" = false,
               "blocks.write" = false,
               codec = 'default',
               column_policy = 'strict',
               "mapping.total_fields.limit" = 1000,
               max_ngram_diff = 1,
               max_shingle_diff = 3,
               "merge.scheduler.max_thread_count" = 1,
               number_of_replicas = '0-1',
               "routing.allocation.enable" = 'all',
               "routing.allocation.total_shards_per_node" = -1,
               "store.type" = 'fs',
               "translog.durability" = 'REQUEST',
               "translog.flush_threshold_size" = 536870912,
               "translog.sync_interval" = 5000,
               "unassigned.node_left.delayed_timeout" = 60000,
               "write.wait_for_active_shards" = '1'
            )""");
    }

    @Test
    public void test_varchar_with_length_limit_is_printed_as_varchar_with_length_in_show_create_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (name varchar(10))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        var node = new TableInfoToAST(table).toStatement();
        assertThat(SqlFormatter.formatSql(node)).contains("\"name\" VARCHAR(10)");
    }

    @Test
    public void test_bit_string_length_is_shown_in_show_create_table_output() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (xs bit(8))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        var node = new TableInfoToAST(table).toStatement();
        assertThat(SqlFormatter.formatSql(node)).contains("\"xs\" BIT(8)");
    }

    @Test
    public void test_generated_expression_on_geo_shape_in_show_create_table_output() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t (g geo_shape generated always as 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        DocTableInfo table = e.resolveTableInfo("t");
        var node = new TableInfoToAST(table).toStatement();
        assertThat(SqlFormatter.formatSql(node)).contains(
            "\"g\" GEO_SHAPE GENERATED ALWAYS AS 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))");
    }

    @Test
    public void test_geo_shape_array_index_definition_is_preserved_in_cluster_state() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t (geo_arr array(geo_shape) INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25'))");
        DocTableInfo table = e.resolveTableInfo("t");
        var node = new TableInfoToAST(table).toStatement();
        assertThat(SqlFormatter.formatSql(node)).contains("""
                "geo_arr" ARRAY(GEO_SHAPE) INDEX USING QUADTREE WITH (
                      distance_error_pct = 0.25,
                      precision = '1m'
                   )
                """);
        Reference reference = table.getReference(ColumnIdent.of("geo_arr"));
        assertThat(reference.valueType()).isEqualTo(new ArrayType<>(DataTypes.GEO_SHAPE));
        GeoReference geoRef = (GeoReference) reference;
        assertThat(geoRef.geoTree()).isEqualTo(GeoShapeType.Names.TREE_QUADTREE);
        assertThat(geoRef.precision()).isEqualTo("1m");
        assertThat(geoRef.distanceErrorPct()).isEqualTo(0.25);
    }
}
