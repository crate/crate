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

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.cast.ToStringFunction;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertFromSubQueryAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(NESTED_PK_TABLE_IDENT.name())).thenReturn(nestedPkTableInfo);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_NESTED_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_NESTED_PARTITIONED_TABLE_INFO);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
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

        SelectAnalyzedStatement selectAnalyzedStatement = (SelectAnalyzedStatement) statement.subQueryRelation();
        List<Symbol> outputSymbols = selectAnalyzedStatement.outputSymbols();
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
        InsertFromSubQueryAnalyzedStatement analysis = (InsertFromSubQueryAnalyzedStatement)
                analyze("insert into users (select * from users where name = 'Trillian')");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithSubQueryColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis = (InsertFromSubQueryAnalyzedStatement)
                analyze("insert into users (" +
                        "  select id, other_id, name, text, no_index, details, " +
                        "      awesome, counters, friends, tags, bytes, shorts, ints, floats " +
                        "  from users " +
                        "  where name = 'Trillian'" +
                        ")");
        assertCompatibleColumns(analysis);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromQueryWithMissingSubQueryColumn() throws Exception {
        analyze("insert into users (" +
                        "  select id, other_id, name, details, awesome, counters, " +
                        "       friends " +
                        "  from users " +
                        "  where name = 'Trillian'" +
                        ")");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromQueryWithMissingInsertColumn() throws Exception {
        analyze("insert into users (id, other_id, name, details, awesome, counters, friends) (" +
                "  select * from users " +
                "  where name = 'Trillian'" +
                ")");
    }


    @Test
    public void testFromQueryWithInsertColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis = (InsertFromSubQueryAnalyzedStatement)
            analyze("insert into users (id, name, details) (" +
                    "  select id, name, details from users " +
                    "  where name = 'Trillian'" +
                    ")");
        assertCompatibleColumns(analysis);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromQueryWithWrongColumnTypes() throws Exception {
        analyze("insert into users (id, details, name) (" +
                "  select id, name, details from users " +
                "  where name = 'Trillian'" +
                ")");
    }

    @Test
    public void testFromQueryWithConvertableInsertColumns() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis = (InsertFromSubQueryAnalyzedStatement)
            analyze("insert into users (id, name) (" +
                "  select id, other_id from users " +
                "  where name = 'Trillian'" +
                ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testFromQueryWithFunctionSubQuery() throws Exception {
        InsertFromSubQueryAnalyzedStatement analysis = (InsertFromSubQueryAnalyzedStatement)
            analyze("insert into users (id) (" +
                "  select count(*) from users " +
                "  where name = 'Trillian'" +
                ")");
        assertCompatibleColumns(analysis);
    }

    @Test
    public void testImplicitTypeCasting() throws Exception {
        InsertFromSubQueryAnalyzedStatement statement = (InsertFromSubQueryAnalyzedStatement)
                analyze("insert into users (id, name) (" +
                        "  select id, other_id from users " +
                        "  where name = 'Trillian'" +
                        ")");


        List<Symbol> outputSymbols = ((SelectAnalyzedStatement) statement.subQueryRelation()).outputSymbols();
        assertThat(statement.columns().size(), is(outputSymbols.size()));
        assertThat(outputSymbols.get(1), instanceOf(Function.class));
        Function castFunction = (Function)outputSymbols.get(1);
        assertThat(castFunction.info().ident().name(), is(ToStringFunction.NAME));
    }
}
