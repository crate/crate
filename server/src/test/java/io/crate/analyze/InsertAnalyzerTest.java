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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.expression.scalar.SubstrFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isAlias;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isInputColumn;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class InsertAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addTable(
                "create table doc.users_generated (" +
                "  id bigint primary key," +
                "  firstname text," +
                "  lastname text," +
                "  name text generated always as firstname || ' ' || lastname" +
                ")")
            .addTable(
                "create table doc.three_pk (" +
                "  a int," +
                "  b int," +
                "  c int," +
                "  d int," +
                "  primary key (a, b, c)" +
                ")")
            .addTable(
                "create table doc.default_column_pk (" +
                "  id int primary key," +
                "  owner text default 'crate' primary key," +
                "  two int default 1+1" +
                ")")
            .addTable("create table doc.nested (o object as (id int primary key), x int)")
            .addTable(
                "create table doc.nested_clustered (" +
                "   obj object(strict) as (id int, name text)" +
                ") clustered by (obj['id'])"
            )
            .build();
    }

    private void assertCompatibleColumns(AnalyzedInsertStatement statement) {
        List<Symbol> outputSymbols = statement.subQueryRelation().outputs();
        assertThat(statement.columns().size(), is(outputSymbols.size()));

        for (int i = 0; i < statement.columns().size(); i++) {
            Symbol subQueryColumn = outputSymbols.get(i);
            assertThat(subQueryColumn, instanceOf(Symbol.class));
            Reference insertColumn = statement.columns().get(i);
            assertThat(
                subQueryColumn.valueType().isConvertableTo(insertColumn.valueType(), false),
                is(true)
            );
        }
    }

    @Test
    public void test_nested_primary_key_can_be_used_as_conflict_target_with_subscript_notation() throws Exception {
        AnalyzedInsertStatement insert =
            e.analyze("insert into doc.nested (o) values (?) on conflict (o['id']) do update set x = x + 1");
        assertThat(
            insert.onDuplicateKeyAssignments(),
            Matchers.hasKey(isReference("x"))
        );
    }

    @Test
    public void test_nested_primary_key_can_be_used_as_conflict_target_with_dotted_column_name() throws Exception {
        AnalyzedInsertStatement insert =
            e.analyze("insert into doc.nested (o) values (?) on conflict (\"o.id\") do update set x = x + 1");
        assertThat(
            insert.onDuplicateKeyAssignments(),
            Matchers.hasKey(isReference("x"))
        );
    }

    @Test
    public void testFromQueryWithoutColumns() throws Exception {
        AnalyzedInsertStatement analysis =
            e.analyze("insert into users (select * from users where name = 'Trillian')");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithSubQueryColumns() throws Exception {
        AnalyzedInsertStatement analysis =
            e.analyze("insert into users (id, name) (" +
                      "  select id, name from users where name = 'Trillian' )");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithMissingSubQueryColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        e.analyze("insert into users (" +
                  "  select id, other_id, name, details, awesome, counters, " +
                  "       friends " +
                  "  from users " +
                  "  where name = 'Trillian'" +
                  ")");

    }

    @Test
    public void testFromQueryWithMissingInsertColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        e.analyze("insert into users (id, other_id, name, details, awesome, counters, friends) (" +
                  "  select * from users " +
                  "  where name = 'Trillian'" +
                  ")");
    }

    @Test
    public void testFromQueryWithInsertColumns() throws Exception {
        AnalyzedInsertStatement analysis =
            e.analyze("insert into users (id, name, details) (" +
                      "  select id, name, details from users " +
                      "  where name = 'Trillian'" +
                      ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithWrongColumnTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        e.analyze("insert into users (id, details, name) (" +
                  "  select id, name, details from users " +
                  "  where name = 'Trillian'" +
                  ")");
    }

    @Test
    public void testFromQueryWithConvertableInsertColumns() throws Exception {
        AnalyzedInsertStatement analysis =
            e.analyze("insert into users (id, name) (" +
                      "  select id, other_id from users " +
                      "  where name = 'Trillian'" +
                      ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithFunctionSubQuery() throws Exception {
        AnalyzedInsertStatement analysis =
            e.analyze("insert into users (id) (" +
                      "  select count(*) from users " +
                      "  where name = 'Trillian'" +
                      ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithOnDuplicateKey() throws Exception {
        var insert = "insert into users (id, name) (select id, name from users) " +
                     "on conflict (id) do update set name = 'Arthur'";

        AnalyzedInsertStatement statement = e.analyze(insert);
        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isLiteral("Arthur", StringType.INSTANCE));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyParameter() throws Exception {
        var insert = "insert into users (id, name) (select id, name from users) " +
                     "on conflict (id) do update set name = ?";

        AnalyzedInsertStatement statement = e.analyze(insert);

        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), instanceOf(ParameterSymbol.class));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyValues() throws Exception {
        var insert = "insert into users (id, name) (select id, name from users) " +
                     "on conflict (id) do update set name = substr(excluded.name, 1, 1)";

        AnalyzedInsertStatement statement = e.analyze(insert);
        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isFunction(SubstrFunction.NAME));
            Function function = (Function) entry.getValue();
            assertThat(function.arguments().get(0), instanceOf(InputColumn.class));
            InputColumn inputColumn = (InputColumn) function.arguments().get(0);
            assertThat(inputColumn.index(), is(1));
            assertThat(inputColumn.valueType(), instanceOf(StringType.class));
        }
    }

    @Test
    public void testFromQueryWithOnConflictAndMultiplePKs() {
        String insertStatement = "insert into three_pk (a, b, c) (select 1, 2, 3) on conflict (a, b, c) do update set d = 1";
        AnalyzedInsertStatement statement = e.analyze(insertStatement);
        assertThat(statement.onDuplicateKeyAssignments().size(), Is.is(1));
        assertThat(statement.onDuplicateKeyAssignments().keySet().iterator().next(), isReference("d"));
        assertThat(statement.onDuplicateKeyAssignments().values().iterator().next(), isLiteral(1));
    }

    @Test
    public void testFromQueryWithUnknownOnDuplicateKeyValues() throws Exception {
        try {
            e.analyze("insert into users (id, name) (select id, name from users) " +
                      "on conflict (id) do update set name = excluded.does_not_exist");
            fail("Analyze passed without a failure.");
        } catch (ColumnUnknownException e) {
            assertThat(e.getMessage(), containsString("Column does_not_exist unknown"));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyPrimaryKeyUpdate() {
        try {
            e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id) do update set id = id + 1");
            fail("Analyze passed without a failure.");
        } catch (ColumnValidationException e) {
            assertThat(e.getMessage(), containsString("Updating a primary key is not supported"));
        }
    }

    @Test
    public void testUpdateOnConflictDoNothingProducesEmptyUpdateAssignments() {
        AnalyzedInsertStatement statement =
            e.analyze("insert into users (id, name) (select 1, 'Jon') on conflict DO NOTHING");
        Map<Reference, Symbol> duplicateKeyAssignments = statement.onDuplicateKeyAssignments();
        assertThat(statement.isIgnoreDuplicateKeys(), is(true));
        assertThat(duplicateKeyAssignments, is(notNullValue()));
        assertThat(duplicateKeyAssignments.size(), is(0));
    }

    @Test
    public void testMissingPrimaryKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column `id` is required but is missing from the insert statement");
        e.analyze("insert into users (name) (select name from users)");
    }

    @Test
    public void testTargetColumnsMustMatchSourceColumnsEvenWithGeneratedColumns() throws Exception {
        /**
         * We want the copy case (insert into target (select * from source)) to work so there is no special logic
         * to exclude generated columns from the target
         */
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of target columns (id, firstname, lastname, name) " +
                                        "of insert statement doesn't match number of source columns (id, firstname, lastname)");
        e.analyze("insert into users_generated (select id, firstname, lastname from users_generated)");
    }

    @Test
    public void testFromQueryWithInvalidConflictTarget() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Conflict target (name) did not match the primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (name) do update set name = excluded.name");
    }

    @Test
    public void testFromQueryWithConflictTargetNotMatchingPK() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([\"id\", \"id2\"]) did not match the number of primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id, id2) do update set name = excluded.name");
    }

    @Test
    public void testInsertFromValuesWithConflictTargetNotMatchingMultiplePKs() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([\"a\", \"b\"]) did not match the number of primary key columns ([a, b, c])");
        e.analyze("insert into three_pk (a, b, c) (select 1, 2, 3) " +
                  "on conflict (a, b) do update set d = 1");
    }

    @Test
    public void testFromQueryWithMissingConflictTarget() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:66: mismatched input 'update' expecting 'NOTHING'");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict do update set name = excluded.name");
    }

    @Test
    public void testFromQueryWithInvalidConflictTargetDoNothing() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Conflict target (name) did not match the primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (name) DO NOTHING");
    }

    @Test
    public void test_query_with_column_that_does_not_exist_as_on_conflict_target() {
        expectedException.expect(ColumnUnknownException.class);
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (invalid_col) DO NOTHING");
    }

    @Test
    public void testFromQueryWithConflictTargetDoNothingNotMatchingPK() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([\"id\", \"id2\"]) did not match the number of primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id, id2) DO NOTHING");
    }

    @Test
    public void testInsertFromValuesWithConflictTargetDoNothingNotMatchingMultiplePKs() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([\"a\", \"b\"]) did not match the number of primary key columns ([a, b, c])");
        e.analyze("insert into three_pk (a, b, c) (select 1, 2, 3) " +
                  "on conflict (a, b) DO NOTHING");
    }

    @Test
    public void testInsertFromQueryMissingPrimaryKeyHavingDefaultExpressionSymbolIsAdded() {
        AnalyzedInsertStatement statement = e.analyze("insert into default_column_pk (id) (select 1)");
        assertCompatibleColumns(statement);

        List<Symbol> pkSymbols = statement.primaryKeySymbols();
        assertThat(pkSymbols, hasSize(2));
        assertThat(pkSymbols.get(0), isInputColumn(0));
        assertThat(pkSymbols.get(1), isLiteral("crate"));
    }

    @Test
    public void test_insert_from_query_with_missing_clustered_by_column() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column `id` is required but is missing from the insert statement");
        e.analyze("insert into users_clustered_by_only (name) (select 'user')");
    }

    @Test
    public void test_insert_from_query_fails_when_source_and_target_types_are_incompatible() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "The type 'bigint' of the insert source 'id' " +
            "is not convertible to the type 'object' of target column 'details'");
        e.analyze("insert into users (id, name, details) (select id, name, id from users)");
    }

    @Test
    public void test_unnest_with_json_str_can_insert_into_object_column() {
        AnalyzedInsertStatement stmt = e.analyze(
            "insert into users (id, address) (select * from unnest([1], ['{\"postcode\":12345}']))");
        assertThat(stmt.subQueryRelation().outputs(), contains(
            isReference("col1"),
            isReference("col2", DataTypes.STRING) // Planner adds a cast projection; text is okay here
        ));
    }

    @Test
    public void test_insert_with_id_in_returning_clause() throws Exception {
        AnalyzedInsertStatement stmt =
            e.analyze("insert into users(id, name) values(1, 'max') returning id");
        assertThat(stmt.outputs(), contains(isReference("id")));
    }

    @Test
    public void test_insert_with_docid_in_returning_clause() throws Exception {
        AnalyzedInsertStatement stmt =
            e.analyze("insert into users(id, name) values(1, 'max') returning _doc");
        assertThat(stmt.outputs(), contains(isReference("_doc")));
    }

    @Test
    public void test_insert_with_id_renamed_in_returning_clause() throws Exception {
        AnalyzedInsertStatement stmt =
            e.analyze("insert into users(id, name) values(1, 'max') returning id as foo");
        assertThat(stmt.outputs(), contains(isAlias("foo", isReference("id"))));
    }

    @Test
    public void test_insert_with_function_in_returning_clause() throws Exception {
        AnalyzedInsertStatement stmt =
            e.analyze("insert into users(id, name) values(1, 'max') returning id + 1 as foo");
        assertThat(stmt.outputs(), contains(isAlias("foo", isFunction("add"))));
    }

    @Test
    public void test_insert_with_returning_all_columns() throws Exception {
        AnalyzedInsertStatement stmt =
            e.analyze("insert into users(id, name) values(1, 'max') returning *");
        assertThat(stmt.outputs().size(), is(17));
    }

    @Test
    public void test_unquoted_on_conflict_columns_are_treated_case_insensitive() {
        //should not throw a ColumnUnknownException
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (ID) do update set name = excluded.name");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id) do update set NAME = excluded.name");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (\"id\") do update set NAME = excluded.name");
    }


    @Test
    public void test_quoted_on_conflict_columns_are_treated_case_sensitive() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (\"ID\") do update set NAME = excluded.name");
    }

    @Test
    public void test_insert_into_table_with_clustered_by_on_nested_column() throws Exception {
        // doesn't raise an error
        e.analyze("insert into doc.nested_clustered (obj) values ({id=4, name='George'})");
    }

}
