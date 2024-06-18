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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class AlterTableAddColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    private AddColumnRequest analyze(String stmt) {
        PlannerContext plannerContext = e.getPlannerContext();
        AnalyzedAlterTableAddColumn analyze = e.analyze(stmt);
        return analyze.bind(
            plannerContext.nodeContext(),
            plannerContext.transactionContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void test_cannot_alter_table_to_add_a_column_definition_of_type_time() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (name text)");

        assertThatThrownBy(() -> analyze("alter table t add column ts time with time zone"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Type `time with time zone` does not support storage");
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        e = SQLExecutor.of(clusterService);

        assertThatThrownBy(() -> e.analyze("alter table sys.shards add column foobar string"))
            .hasMessage("The relation \"sys.shards\" doesn't support or allow ALTER " +
                "operations");
    }

    @Test
    public void testAddColumnOnSinglePartitionNotAllowed() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS);

        assertThatThrownBy(() -> e.analyze("alter table parted partition (date = 1395874800000) add column foobar string"))
            .hasMessage("Adding a column to a single partition is not supported");
    }

    @Test
    public void testAddColumnWithAnalyzerAndNonStringType() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table users (name text)");

        assertThatThrownBy(() -> analyze("alter table users add column foobar object as (age int index using fulltext)"))
            .hasMessage("Can't use an Analyzer on column foobar['age'] because analyzers are only allowed " +
                "on columns of type \"" + DataTypes.STRING.getName() + "\" of the unbound length limit.");
    }

    @Test
    public void testAddFulltextIndex() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table users (name text)");

        assertThatThrownBy(() -> e.analyze("alter table users add column index ft_foo using fulltext (name)"))
            .isExactlyInstanceOf(ParsingException.class);
    }

    @Test
    public void testAddColumnThatExistsAlready() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table users (name text)");

        assertThatThrownBy(() -> analyze("alter table users add column name string"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The table doc.users already has a column named name");
    }

    @Test
    public void testAddPrimaryKeyColumnWithArrayTypeUnsupported() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table users (id bigint primary key, name text)");

        assertThatThrownBy(() -> analyze("alter table users add column newpk array(string) primary key"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use column \"newpk\" with type \"text_array\" as primary key");
    }

    @Test
    public void test_cannot_add_named_primary_key_constraint_to_existing_table() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int primary key)");

        assertThatThrownBy(() -> analyze("alter table t add column b int constraint c_1 primary key"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot alter the name of PRIMARY KEY constraint");
    }

    @Test
    public void testAddColumnWithCheckConstraintFailsBecauseItRefersToAnotherColumn() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table users (id bigint primary key, name text)");

        assertThatThrownBy(() -> analyze("alter table users add column bazinga int constraint bazinga_check check(id > 0)"))
            .hasMessage("CHECK constraint on column `bazinga` cannot refer to column `id`. Use full path to refer " +
                        "to a sub-column or a table check constraint instead");
    }

    @Test
    public void add_multiple_columns_pkey_indices_referring_to_correct_ref_indices() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE tbl (x int)");

        AddColumnRequest request = analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN o['a']['c'] int primary key");

        assertThat(request.pKeyIndices()).hasSize(2);
        assertThat(request.references()).hasSize(4); // 2 leaves (b, c) and their common parents (o, a).

        List<String> pKeyColNames = new ArrayList<>();
        for (IntCursor cursor: request.pKeyIndices()) {
            var ref = request.references().get(cursor.value);
            pKeyColNames.add(ref.column().leafName());
        }
        assertThat(pKeyColNames).containsExactlyInAnyOrder("b", "c");
    }

    @Test
    public void testAlterTableAddColumnWithNullConstraint() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE tbl (col1 INT, col2 INT)");

        assertThatThrownBy(() -> analyze("ALTER TABLE tbl ADD COLUMN col3 INT PRIMARY KEY NULL"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"col3\" is declared as PRIMARY KEY, therefore, cannot be declared NULL");

        assertThatThrownBy(() -> analyze("ALTER TABLE tbl ADD COLUMN col3 INT NULL PRIMARY KEY"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"col3\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY");

        assertThatThrownBy(() -> analyze("ALTER TABLE tbl ADD COLUMN col3 INT NOT NULL NULL"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"col3\" is declared as NOT NULL, therefore, cannot be declared NULL");

        assertThatThrownBy(() -> analyze("ALTER TABLE tbl ADD COLUMN col3 INT NULL NOT NULL"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"col3\" is declared NULL, therefore, cannot be declared NOT NULL");

        AnalyzedAlterTableAddColumn analysis = e.analyze("""
                    ALTER TABLE tbl
                        ADD COLUMN col3 INT NULL
                """);

        Map<ColumnIdent, RefBuilder> columns = analysis.columns();
        RefBuilder rb = columns.get(ColumnIdent.of("col3"));
        assertThat(rb.isExplicitlyNull()).isTrue();
    }

    @Test
    public void add_multiple_columns_adding_same_name_primitive_throws_an_exception() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE tbl (x int)");

        // same name, same type
        assertThatThrownBy(() -> e.analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN int_col INTEGER, ADD COLUMN int_col INTEGER"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column \"int_col\" specified more than once");

        // only same name, different type
        assertThatThrownBy(() -> e.analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN col INTEGER, ADD COLUMN col TEXT"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column \"col\" specified more than once");
    }

    @Test
    public void test_check_constraint_on_nested_object_sub_column_has_correct_type_and_expression() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (i int, o object)");

        var addColumnRequest = analyze("alter table t add column o1 object as (o2 object as (b int check (o1['o2']['b'] > 100)))");
        assertThat(addColumnRequest.references()).satisfiesExactly(
            o1 -> assertThat(o1).isReference().hasName("o1"),
            o1 -> assertThat(o1).isReference().hasName("o1['o2']"),
            o1 -> assertThat(o1).isReference().hasName("o1['o2']['b']").hasType(DataTypes.INTEGER)
        );
        assertThat(addColumnRequest.checkConstraints()).containsValue("\"o1\"['o2']['b'] > 100");
    }

    @Test
    public void test_check_constraint_cannot_be_added_to_nested_object_sub_column_without_full_path() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (i int, o object)");

        assertThatThrownBy(
            () -> analyze("alter table t add column o1 object as (o2 object as (b int check (b > 100)))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("CHECK constraint on column `o1['o2']['b']` cannot refer to column `b`. Use full path to " +
                        "refer to a sub-column or a table check constraint instead");
    }
}
