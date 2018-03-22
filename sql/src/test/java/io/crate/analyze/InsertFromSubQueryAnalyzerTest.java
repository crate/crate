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

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.expression.scalar.SubstrFunction;
import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class InsertFromSubQueryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        SQLExecutor.Builder builder = SQLExecutor.builder(clusterService).enableDefaultTables();

        TableIdent usersGeneratedIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "users_generated");
        TestingTableInfo.Builder usersGenerated = new TestingTableInfo.Builder(usersGeneratedIdent, SHARD_ROUTING)
            .add("id", DataTypes.LONG)
            .add("firstname", DataTypes.STRING)
            .add("lastname", DataTypes.STRING)
            .addGeneratedColumn("name", DataTypes.STRING, "firstname || ' ' || lastname", false)
            .addPrimaryKey("id");
        builder.addDocTable(usersGenerated);
        e = builder.build();
    }

    private void assertCompatibleColumns(InsertFromSubQueryAnalyzedStatement statement) {
        List<Symbol> outputSymbols = statement.subQueryRelation().querySpec().outputs();
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
            e.analyze("insert into users (" +
                      "  select id, other_id, name, text, no_index, details, address, " +
                      "      awesome, counters, friends, tags, bytes, shorts, date, shape, ints, floats " +
                      "  from users " +
                      "  where name = 'Trillian'" +
                      ")");
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
        InsertFromSubQueryAnalyzedStatement statement =
            e.analyze("insert into users (id, name) (select id, name from users) " +
                      "on duplicate key update name = 'Arthur'");

        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isLiteral("Arthur", StringType.INSTANCE));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyParameter() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement =
            e.analyze("insert into users (id, name) (select id, name from users) " +
                      "on duplicate key update name = ?",
                new Object[]{"Arthur"});

        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isLiteral("Arthur", StringType.INSTANCE));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyValues() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement =
            e.analyze("insert into users (id, name) (select id, name from users) " +
                      "on duplicate key update name = substr(values (name), 1, 1)");

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
    public void testFromQueryWithUnknownOnDuplicateKeyValues() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column does_not_exist unknown");
        e.analyze("insert into users (id, name) (select id, name from users) " +
                  "on duplicate key update name = values (does_not_exist)");
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyPrimaryKeyUpdate() {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Updating a primary key is not supported");
        e.analyze("insert into users (id, name) (select 1, 'Arthur') on duplicate key update id = id + 1");
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
}
