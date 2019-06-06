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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.expression.scalar.SubstrFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.StringType;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isInputColumn;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class InsertFromSubQueryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

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
            .build();
    }

    private void assertCompatibleColumns(InsertFromSubQueryAnalyzedStatement statement) {
        List<Symbol> outputSymbols = statement.subQueryRelation().outputs();
        assertThat(statement.columns().size(), is(outputSymbols.size()));

        for (int i = 0; i < statement.columns().size(); i++) {
            Symbol subQueryColumn = outputSymbols.get(i);
            assertThat(subQueryColumn, instanceOf(Symbol.class));
            Reference insertColumn = statement.columns().get(i);
            assertThat(
                subQueryColumn.valueType().isConvertableTo(insertColumn.valueType()),
                is(true)
            );
        }
    }

    @Test
    public void testFromQueryWithoutColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
            e.analyze("insert into users (select * from users where name = 'Trillian')");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithSubQueryColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
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
        InsertFromSubQueryAnalyzedStatement analysis =
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
        InsertFromSubQueryAnalyzedStatement analysis =
            e.analyze("insert into users (id, name) (" +
                      "  select id, other_id from users " +
                      "  where name = 'Trillian'" +
                      ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithFunctionSubQuery() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
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

        InsertFromSubQueryAnalyzedStatement statement = e.analyze(insert);
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

        InsertFromSubQueryAnalyzedStatement statement = e.analyze(insert, new Object[]{"Arthur"});

        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isLiteral("Arthur", StringType.INSTANCE));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyValues() throws Exception {
        var insert = "insert into users (id, name) (select id, name from users) " +
                     "on conflict (id) do update set name = substr(excluded.name, 1, 1)";

        InsertFromSubQueryAnalyzedStatement statement = e.analyze(insert);
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
        InsertFromSubQueryAnalyzedStatement statement = e.analyze(insertStatement);
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
        InsertFromSubQueryAnalyzedStatement statement =
            e.analyze("insert into users (id, name) (select 1, 'Jon') on conflict DO NOTHING");
        Map<Reference, Symbol> duplicateKeyAssignments = statement.onDuplicateKeyAssignments();
        assertThat(statement.isIgnoreDuplicateKeys(), is(true));
        assertThat(duplicateKeyAssignments, is(notNullValue()));
        assertThat(duplicateKeyAssignments.size(), is(0));
    }

    @Test
    public void testMissingPrimaryKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column \"id\" is required but is missing from the insert statement");
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
        expectedException.expectMessage("Conflict target ([id2]) did not match the primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id2) do update set name = excluded.name");
    }

    @Test
    public void testFromQueryWithConflictTargetNotMatchingPK() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([id, id2]) did not match the number of primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id, id2) do update set name = excluded.name");
    }

    @Test
    public void testInsertFromValuesWithConflictTargetNotMatchingMultiplePKs() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([a, b]) did not match the number of primary key columns ([a, b, c])");
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
        expectedException.expectMessage("Conflict target ([id2]) did not match the primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id2) DO NOTHING");
    }

    @Test
    public void testFromQueryWithConflictTargetDoNothingNotMatchingPK() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([id, id2]) did not match the number of primary key columns ([id])");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on conflict (id, id2) DO NOTHING");
    }

    @Test
    public void testInsertFromValuesWithConflictTargetDoNothingNotMatchingMultiplePKs() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Number of conflict targets ([a, b]) did not match the number of primary key columns ([a, b, c])");
        e.analyze("insert into three_pk (a, b, c) (select 1, 2, 3) " +
                  "on conflict (a, b) DO NOTHING");
    }

    @Test
    public void testInsertFromQueryMissingPrimaryKeyHavingDefaultExpressionSymbolIsAdded() {
        InsertFromSubQueryAnalyzedStatement statement = e.analyze("insert into default_column_pk (id) (select 1)");
        assertCompatibleColumns(statement);

        List<Symbol> pkSymbols = statement.primaryKeySymbols();
        assertThat(pkSymbols, hasSize(2));
        assertThat(pkSymbols.get(0), isInputColumn(0));
        assertThat(pkSymbols.get(1), isLiteral("crate"));
    }
}
