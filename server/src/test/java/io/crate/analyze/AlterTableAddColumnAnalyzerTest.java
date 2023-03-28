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

import static io.crate.planner.node.ddl.AlterTableAddColumnPlan.validate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.cursors.IntCursor;

import org.junit.Test;

import io.crate.data.Row;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTableAddColumnPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

import static org.assertj.core.api.Assertions.assertThat;

public class AlterTableAddColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    private void analyze(String stmt) {
        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());
        validate(
            e.analyze(stmt),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver()
        );
    }

    @Test
    public void test_cannot_alter_table_to_add_a_column_definition_of_type_time() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (name text)")
            .build();

        assertThatThrownBy(() -> analyze("alter table t add column ts time with time zone"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot use the type `time with time zone` for column: ts");
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        e = SQLExecutor.builder(clusterService).build();

        assertThatThrownBy(() -> e.analyze("alter table sys.shards add column foobar string"))
            .hasMessage("The relation \"sys.shards\" doesn't support or allow ALTER " +
                "operations, as it is read-only.");
    }

    @Test
    public void testAddColumnOnSinglePartitionNotAllowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .build();

        assertThatThrownBy(() -> e.analyze("alter table parted partition (date = 1395874800000) add column foobar string"))
            .hasMessage("Adding a column to a single partition is not supported");
    }

    @Test
    public void testAddColumnWithAnalyzerAndNonStringType() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        assertThatThrownBy(() -> analyze("alter table users add column foobar object as (age int index using fulltext)"))
            .hasMessage("Can't use an Analyzer on column foobar['age'] because analyzers are only allowed " +
                "on columns of type \"" + DataTypes.STRING.getName() + "\" of the unbound length limit.");
    }

    @Test
    public void testAddFulltextIndex() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        assertThatThrownBy(() -> e.analyze("alter table users add column index ft_foo using fulltext (name)"))
            .isExactlyInstanceOf(ParsingException.class);
    }

    @Test
    public void testAddColumnThatExistsAlready() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        assertThatThrownBy(() -> analyze("alter table users add column name string"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The table doc.users already has a column named name");
    }

    @Test
    public void testAddPrimaryKeyColumnWithArrayTypeUnsupported() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();

        assertThatThrownBy(() -> analyze("alter table users add column newpk array(string) primary key"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use columns of type \"array\" as primary key");
    }

    @Test
    public void testAddColumnWithCheckConstraintFailsBecauseItRefersToAnotherColumn() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();

        assertThatThrownBy(() -> analyze("alter table users add column bazinga int constraint bazinga_check check(id > 0)"))
            .hasMessage("CHECK expressions defined in this context cannot refer to other columns: id");
    }

    @Test
    public void add_multiple_columns_pkey_indices_referring_to_correct_ref_indices() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (x int)")
            .build();

        AnalyzedAlterTableAddColumn analyzedAlterTableAddColumn =
            e.analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN o['a']['c'] int primary key");

        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());

        var tableElements = validate(
            analyzedAlterTableAddColumn,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver()
        );

        var request = AlterTableAddColumnPlan.createRequest(tableElements, analyzedAlterTableAddColumn.tableInfo().ident());

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
    public void add_multiple_columns_adding_same_name_primitive_throws_an_exception() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (x int)")
            .build();

        // same name, same type
        assertThatThrownBy(() -> e.analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN int_col INTEGER, ADD COLUMN int_col INTEGER"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column \"int_col\" specified more than once");

        // only same name, different type
        assertThatThrownBy(() -> e.analyze("ALTER TABLE tbl ADD COLUMN o['a']['b'] int primary key, ADD COLUMN col INTEGER, ADD COLUMN col TEXT"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column \"col\" specified more than once");
    }
}
