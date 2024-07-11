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

package io.crate.integrationtests;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedOptimizerRules;

@UseRandomizedOptimizerRules(0)
public class SwapTableITest extends IntegTestCase {

    @Test
    public void test_swap_two_simple_tables() {
        execute("create table source (s int)");
        execute("create table target (t double)");

        execute("insert into source (s) values (1)");
        execute("insert into target (t) values (2)");
        execute("refresh table source, target");

        execute("alter cluster swap table source to target");
        execute("select * from source");
        assertThat(printedTable(response.rows()))
                .isEqualTo("2.0\n");
        execute("select * from target");
        assertThat(printedTable(response.rows()))
                .isEqualTo("1\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("source\ntarget\n");
    }

    @Test
    public void test_swap_two_simple_tables_with_drop_source() {
        execute("create table source (s int)");
        execute("create table target (t double)");

        execute("insert into source (s) values (1)");
        execute("insert into target (t) values (2)");
        execute("refresh table source, target");

        execute("alter cluster swap table source to target with (drop_source = ?)", $(true));
        execute("select * from target");
        assertThat(printedTable(response.rows()))
                .isEqualTo("1\n");

        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("target\n");

        Asserts.assertSQLError(() -> execute("select * from source"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'source' unknown");
    }

    @Test
    public void test_swap_source_partitioned_target_nonpartitioned() {
        execute("create table source (s int) partitioned by (s)");
        execute("create table target (t int)");

        execute("insert into source (s) values (1),(2)");
        execute("insert into target (t) values (3),(4)");
        execute("refresh table source, target");

        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(2L);
        String part1Ident = (String) response.rows()[0][0];
        String part2Ident = (String) response.rows()[1][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| source\n" +
                           part2Ident + "| source\n");

        execute("alter cluster swap table source to target");
        assertThat(printedTable(execute("select * from source order by t").rows()))
                .isEqualTo("3\n4\n");
        assertThat(printedTable(execute("select * from target order by s").rows()))
                .isEqualTo("1\n2\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("source\ntarget\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| target\n" +
                           part2Ident + "| target\n");
    }

    @Test
    public void test_swap_source_partitioned_target_nonpartitioned_with_drop_source() {
        execute("create table source (s int) partitioned by (s)");
        execute("create table target (t int)");

        execute("insert into source (s) values (1),(2)");
        execute("insert into target (t) values (3),(4)");
        execute("refresh table source, target");

        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(2L);
        String part1Ident = (String) response.rows()[0][0];
        String part2Ident = (String) response.rows()[1][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| source\n" +
                           part2Ident + "| source\n");

        execute("alter cluster swap table source to target with (drop_source = ?)", $(true));
        assertThat(printedTable(execute("select * from target order by s").rows()))
                .isEqualTo("1\n2\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("target\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| target\n" +
                           part2Ident + "| target\n");

        Asserts.assertSQLError(() -> execute("select * from source"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'source' unknown");
    }

    @Test
    public void test_swap_source_nonpartitioned_target_partitioned() {
        execute("create table source (s int)");
        execute("create table target (t int) partitioned by(t)");

        execute("insert into source (s) values (1),(2)");
        execute("insert into target (t) values (3),(4)");
        execute("refresh table source, target");

        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(2L);
        String part1Ident = (String) response.rows()[0][0];
        String part2Ident = (String) response.rows()[1][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| target\n" +
                           part2Ident + "| target\n");

        execute("alter cluster swap table source to target");
        assertThat(printedTable(execute("select * from source order by t").rows()))
                .isEqualTo("3\n4\n");
        assertThat(printedTable(execute("select * from target order by s").rows()))
                .isEqualTo("1\n2\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("source\ntarget\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| source\n" +
                           part2Ident + "| source\n");
    }

    @Test
    @TestLogging("org.elasticsearch.index.shard.IndexShard:TRACE,io.crate.metadata.doc.DocTableInfoFactory:DEBUG,io.crate.metadata.doc.DocTableInfo:DEBUG,io.crate.metadata.doc.DocSchemaInfo:DEBUG")
    public void test_swap_source_nonpartitioned_target_partitioned_with_drop_source() {
        execute("create table source (s int)");
        execute("create table target (t int) partitioned by(t)");

        execute("insert into source (s) values (1),(2)");
        execute("insert into target (t) values (3),(4)");
        execute("refresh table source, target");

        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(2L);
        String part1Ident = (String) response.rows()[0][0];
        String part2Ident = (String) response.rows()[1][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| target\n" +
                           part2Ident + "| target\n");

        execute("insert into source (s) values (5), (6)");

        execute("alter cluster swap table source to target with (drop_source = ?)", $(true));
        execute("insert into target (s) values (7), (8)");
        execute("refresh table target");
        assertThat(printedTable(execute("select * from target order by s").rows()))
                .isEqualTo("1\n2\n5\n6\n7\n8\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("target\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(0L);

        Asserts.assertSQLError(() -> execute("select * from source"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'source' unknown");
    }

    @Test
    public void test_swap_two_partitioned_tables_where_target_is_empty() {
        execute("create table source (s int) partitioned by (s)");
        execute("create table target (t int) partitioned by (t)");

        execute("insert into source (s) values (1), (2)");
        execute("refresh table source");
        execute("select partition_ident, table_name from information_schema.table_partitions order by 1");
        assertThat(response.rowCount()).isEqualTo(2L);
        String part1Ident = (String) response.rows()[0][0];
        String part2Ident = (String) response.rows()[1][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| source\n" +
                           part2Ident + "| source\n");

        execute("alter cluster swap table source to target");
        execute("select * from source");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("select * from target order by s");
        assertThat(printedTable(response.rows()))
                .isEqualTo("1\n2\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("source\ntarget\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1Ident + "| target\n" +
                           part2Ident + "| target\n");
    }

    @Test
    public void test_swap_two_partitioned_tables_with_drop_source() {
        execute("create table source (s int) partitioned by(s)");
        execute("create table target (t int) partitioned by(t)");

        execute("insert into source (s) values (1),(2)");
        execute("insert into target (t) values (3),(4)");
        execute("refresh table source, target");

        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(response.rowCount()).isEqualTo(4L);
        String part1IdentSource = (String) response.rows()[0][0];
        String part2IdentSource = (String) response.rows()[1][0];
        String part1IdentTarget = (String) response.rows()[2][0];
        String part2IdentTarget = (String) response.rows()[3][0];
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1IdentSource + "| source\n" +
                           part2IdentSource + "| source\n" +
                           part1IdentTarget + "| target\n" +
                           part2IdentTarget + "| target\n");

        execute("alter cluster swap table source to target with (drop_source = ?)", $(true));
        assertThat(printedTable(execute("select * from target order by s").rows()))
                .isEqualTo("1\n2\n");
        assertThat(printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('source', 'target') order by 1").rows()))
                .isEqualTo("target\n");
        execute("select partition_ident, table_name from information_schema.table_partitions where table_name in ('source', 'target') order by 1");
        assertThat(printedTable(response.rows()))
                .isEqualTo(part1IdentSource + "| target\n" +
                           part2IdentSource + "| target\n");

        Asserts.assertSQLError(() -> execute("select * from source"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'source' unknown");
    }
}
