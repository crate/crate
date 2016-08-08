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

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.SubstrFunction;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.elasticsearch.common.inject.Module;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

public class InsertFromSubQueryAnalyzerTest extends BaseAnalyzerTest {

    @Mock
    private SchemaInfo schemaInfo;

    private class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            when(schemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    @Before
    public void initGeneratedColumnTable() throws Exception {
        TableIdent usersGeneratedIdent = new TableIdent(null, "users_generated");
        DocTableInfo usersGenerated = new TestingTableInfo.Builder(usersGeneratedIdent, SHARD_ROUTING)
                .add("id", DataTypes.LONG)
                .add("firstname", DataTypes.STRING)
                .add("lastname", DataTypes.STRING)
                .addGeneratedColumn("name", DataTypes.STRING, "firstname || ' ' || lastname", false)
                .addPrimaryKey("id").build(injector.getInstance(Functions.class));
        when(schemaInfo.getTableInfo(usersGeneratedIdent.name())).thenReturn(usersGenerated);
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule()
        ));
        return modules;
    }

    private void assertCompatibleColumns(InsertFromSubQueryAnalyzedStatement statement) {

        List<Symbol> outputSymbols = ((QueriedDocTable)statement.subQueryRelation()).querySpec().outputs();
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
                analyze("insert into users (select * from users where name = 'Trillian')");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithSubQueryColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
                analyze("insert into users (" +
                        "  select id, other_id, name, text, no_index, details, " +
                        "      awesome, counters, friends, tags, bytes, shorts, shape, ints, floats " +
                        "  from users " +
                        "  where name = 'Trillian'" +
                        ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithMissingSubQueryColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("insert into users (" +
                "  select id, other_id, name, details, awesome, counters, " +
                "       friends " +
                "  from users " +
                "  where name = 'Trillian'" +
                ")");

    }

    @Test
    public void testFromQueryWithMissingInsertColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("insert into users (id, other_id, name, details, awesome, counters, friends) (" +
                "  select * from users " +
                "  where name = 'Trillian'" +
                ")");
    }


    @Test
    public void testFromQueryWithInsertColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
            analyze("insert into users (id, name, details) (" +
                    "  select id, name, details from users " +
                    "  where name = 'Trillian'" +
                    ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithWrongColumnTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("insert into users (id, details, name) (" +
                "  select id, name, details from users " +
                "  where name = 'Trillian'" +
                ")");
    }

    @Test
    public void testFromQueryWithConvertableInsertColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
            analyze("insert into users (id, name) (" +
                "  select id, other_id from users " +
                "  where name = 'Trillian'" +
                ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithFunctionSubQuery() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis =
            analyze("insert into users (id) (" +
                "  select count(*) from users " +
                "  where name = 'Trillian'" +
                ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testImplicitTypeCasting() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement =
                analyze("insert into users (id, name, shape) (" +
                        "  select id, other_id, name from users " +
                        "  where name = 'Trillian'" +
                        ")");

        List<Symbol> outputSymbols = ((QueriedDocTable)statement.subQueryRelation()).querySpec().outputs();
        assertThat(statement.columns().size(), is(outputSymbols.size()));
        assertThat(outputSymbols.get(1), instanceOf(Function.class));
        Function castFunction = (Function)outputSymbols.get(1);
        assertThat(castFunction, TestingHelpers.isFunction(CastFunctionResolver.FunctionNames.TO_STRING));
        Function geoCastFunction = (Function)outputSymbols.get(2);
        assertThat(geoCastFunction, TestingHelpers.isFunction(CastFunctionResolver.FunctionNames.TO_GEO_SHAPE));
    }

    @Test
    public void testFromQueryWithOnDuplicateKey() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement =
                analyze("insert into users (id, name) (select id, name from users) " +
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
                analyze("insert into users (id, name) (select id, name from users) " +
                        "on duplicate key update name = ?",
                        new Object[]{ "Arthur" });

        Assert.assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        for (Map.Entry<Reference, Symbol> entry : statement.onDuplicateKeyAssignments().entrySet()) {
            assertThat(entry.getKey(), isReference("name"));
            assertThat(entry.getValue(), isLiteral("Arthur", StringType.INSTANCE));
        }
    }

    @Test
    public void testFromQueryWithOnDuplicateKeyValues() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement =
                analyze("insert into users (id, name) (select id, name from users) " +
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
        analyze("insert into users (id, name) (select id, name from users) " +
                "on duplicate key update name = values (does_not_exist)");
    }

    @Test
    public void testMissingPrimaryKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column \"id\" is required but is missing from the insert statement");
        analyze("insert into users (name) (select name from users)");
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
        analyze("insert into users_generated (select id, firstname, lastname from users_generated)");
    }
}
