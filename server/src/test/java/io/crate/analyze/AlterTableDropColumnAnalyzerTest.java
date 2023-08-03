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

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class AlterTableDropColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Test
    public void test_drop_simple_column() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int, b int)")
            .build();

        AnalyzedAlterTableDropColumn d = e.analyze("ALTER TABLE t DROP COLUMN a");
        assertThat(d.columns()).satisfiesExactly(
            r -> assertThat(r).isReference().hasName("a"));
    }

    @Test
    public void test_drop_multiple_simple_columns() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int, b int, c int)")
            .build();

        AnalyzedAlterTableDropColumn d = e.analyze("ALTER TABLE t DROP COLUMN a, DROP b");
        assertThat(d.table().ident().name()).isEqualTo("t");
        assertThat(d.columns()).satisfiesExactly(
            r -> assertThat(r).isReference().hasName("a"),
            r -> assertThat(r).isReference().hasName("b"));
    }

    @Test
    public void test_drop_column_part_of_simple_index() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int, b text INDEX USING fulltext)")
            .build();

        AnalyzedAlterTableDropColumn d = e.analyze("ALTER TABLE t DROP COLUMN b");
        assertThat(d.columns()).satisfiesExactly(
            r -> assertThat(r).isReference().hasName("b"));
    }

    @Test
    public void test_drop_sub_column() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int, o object AS (oo object AS(ooa int, oob long)))")
            .build();

        AnalyzedAlterTableDropColumn d = e.analyze("ALTER TABLE t DROP COLUMN o['oo']['ooa']");
        assertThat(d.table().ident().name()).isEqualTo("t");
        assertThat(d.columns()).satisfiesExactly(
            r -> assertThat(r)
                .isReference()
                .hasColumnIdent(new ColumnIdent("o", List.of("oo", "ooa")))
                .hasTableIdent(d.table().ident())
                .hasType(DataTypes.INTEGER)
        );
    }

    @Test
    public void test_drop_not_existing_column_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (a int, b int, c int)")
            .addTable("create table t2 (o object AS (oa int, oo object AS(ooa int, oob int)))")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t1 DROP COLUMN d"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column d unknown");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP COLUMN o['ob']"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column o['ob'] unknown");
        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP COLUMN o['oo']['ooc']"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column o['oo']['ooc'] unknown");
    }

    @Test
    public void test_drop_not_existing_column_with_if_exists() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (a int, b int, c int)")
            .addTable("create table t2 (o object AS (oa int, oo object AS(ooa int, oob int)))")
            .build();

        AnalyzedAlterTableDropColumn d1 = e.analyze("ALTER TABLE t1 DROP COLUMN IF EXISTS d, DROP IF EXISTS e");
        assertThat(d1.table().ident().name()).isEqualTo("t1");
        assertThat(d1.columns()).isEmpty();

        AnalyzedAlterTableDropColumn d2 = e.analyze("ALTER TABLE t1 DROP COLUMN b, DROP IF EXISTS d");
        assertThat(d2.table().ident().name()).isEqualTo("t1");
        assertThat(d2.columns()).satisfiesExactly(
            r -> assertThat(r).isReference().hasName("b"));

        AnalyzedAlterTableDropColumn d3 = e.analyze("ALTER TABLE t2 DROP COLUMN o['oa'], DROP COLUMN IF EXISTS o['ob'], " +
                         "DROP o['oo']['ooa'], DROP IF EXISTS o['oo']['ooc']");
        assertThat(d3.table().ident().name()).isEqualTo("t2");
        assertThat(d3.columns()).satisfiesExactly(
            r -> assertThat(r)
                .isReference()
                .hasColumnIdent(new ColumnIdent("o", "oa"))
                .hasTableIdent(d3.table().ident())
                .hasType(DataTypes.INTEGER),
            r -> assertThat(r)
                .isReference()
                .hasColumnIdent(new ColumnIdent("o", List.of("oo", "ooa")))
                .hasTableIdent(d3.table().ident())
                .hasType(DataTypes.INTEGER)
        );
    }

    @Test
    public void test_drop_all_columns_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (a int, b int, c int)")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t DROP COLUMN a, DROP b, DROP COLUMN c"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Dropping all columns of a table is not allowed");
    }

    @Test
    public void test_drop_column_from_system_table_is_not_allowed() {
        e = SQLExecutor.builder(clusterService).build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE sys.shards DROP COLUMN foobar"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.shards\" doesn't support or allow ALTER " +
                "operations, as it is read-only.");
    }

    @Test
    public void test_drop_clustered_by_column_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t1 (a int) CLUSTERED BY (a)")
            .addTable("CREATE TABLE t2 (o object AS(oo object AS(ooa int))) CLUSTERED BY (o['oo']['ooa'])")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t1 DROP COLUMN a"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: a which is used in 'CLUSTERED BY' is not allowed");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP COLUMN o['oo']['ooa']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: o['oo']['ooa'] which is used in 'CLUSTERED BY' is not allowed");
    }

    @Test
    public void test_drop_column_from_single_partition_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE parted partition (date = 1395874800000) DROP COLUMN name"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping a column from a single partition is not supported");
    }

    @Test
    public void test_drop_partition_by_column_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addPartitionedTable("CREATE TABLE t2 (o object AS (oo object AS(ooa int))) PARTITIONED BY (o['oo']['ooa'])",
                                 new PartitionName(new RelationName("doc", "t2"), singletonList("1")).asIndexName())
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE parted DROP COLUMN date"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: date which is part of the 'PARTITIONED BY' columns is not allowed");
        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP COLUMN o['oo']['ooa']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: o['oo']['ooa'] which is part of the 'PARTITIONED BY' columns is not allowed");
    }

    @Test
    public void test_drop_pk_column_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_MULTI_PK_DEFINITION)
            .addTable("CREATE TABLE t2 (o object AS(oo object AS(ooa int)), PRIMARY KEY (o['oo']['ooa']))")
            .build();

        // id is also part of clustered by, but pk is checked first
        assertThatThrownBy(() -> e.analyze("ALTER TABLE users_multi_pk DROP COLUMN id"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: id which is part of the PRIMARY KEY is not allowed");
        assertThatThrownBy(() -> e.analyze("ALTER TABLE users_multi_pk DROP name"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: name which is part of the PRIMARY KEY is not allowed");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP COLUMN o['oo']['ooa']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: o['oo']['ooa'] which is part of the PRIMARY KEY is not allowed");
    }

    @Test
    public void test_drop_column_used_in_a_generated_col_is_not_allowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t(a int, b generated always as (a + 1))")
            .addTable("CREATE TABLE t1(o object AS (oo object AS(ooa int)), b generated always as (o['oo']['ooa'] + 1))")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t DROP a"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: a which is used to produce values for generated column is not allowed");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t1 DROP o['oo']['ooa']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: o['oo']['ooa'] which is used to produce values for generated column is not allowed");
    }

    @Test
    public void test_drop_column_used_in_a_named_index() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t1(a text, b text, INDEX ft USING fulltext(a, b))")
            .addTable("CREATE TABLE t2(a text, b text, INDEX ft USING fulltext(b))")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t1 DROP b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: b which is part of INDEX: ft is not allowed");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t2 DROP b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Dropping column: b which is part of INDEX: ft is not allowed");
    }

    @Test
    public void add_multiple_columns_adding_same_name_primitive_throws_an_exception() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t (o object AS(oo object AS(ooa int, oob int)))")
            .build();

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t DROP o['oo']['ooa'], DROP o['oo']['oob'], DROP o['oo']['ooa']"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column \"o['oo']['ooa']\" specified more than once");
    }
}
