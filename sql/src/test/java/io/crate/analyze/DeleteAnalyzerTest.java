/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.RelationOutput;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
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
                        new OperatorModule())
        );
        return modules;
    }

    @Override
    protected DeleteAnalyzedStatement analyze(String statement) {
        return (DeleteAnalyzedStatement)super.analyze(statement);
    }

    protected DeleteAnalyzedStatement analyze(String statement, Object[][] bulkArgs) {
        return (DeleteAnalyzedStatement) super.analyze(statement, bulkArgs);
    }

    @Test
    public void testDeleteWhere() throws Exception {
        DeleteAnalyzedStatement statement = analyze("delete from users where name='Trillian'");
        TableInfo tableInfo = ((TableRelation) statement.analyzedRelation()).tableInfo();
        assertThat(TEST_DOC_TABLE_IDENT, equalTo(tableInfo.ident()));

        assertThat(tableInfo.rowGranularity(), is(RowGranularity.DOC));

        Function whereClause = (Function)statement.whereClauses.get(0).query();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        assertThat(RelationOutput.unwrap(whereClause.arguments().get(0)), IsInstanceOf.instanceOf(Reference.class));

        assertLiteralSymbol(whereClause.arguments().get(1), "Trillian");
    }

    @Test( expected = UnsupportedOperationException.class)
    public void testDeleteSystemTable() throws Exception {
        analyze("delete from sys.nodes where name='Trillian'");
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testDeleteWhereSysColumn() throws Exception {
        analyze("delete from users where sys.nodes.id = 'node_1'");
    }

    @Test
    public void testDeleteWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyze("delete from parted where date = 1395874800000");
        assertThat(statement.whereClauses().get(0).hasQuery(), is(true));
        assertThat(statement.whereClauses().get(0).noMatch(), is(false));
    }

    @Test
    public void testDeleteTableAlias() throws Exception {
        DeleteAnalyzedStatement expectedStatement = analyze("delete from users where name='Trillian'");
        DeleteAnalyzedStatement actualStatement = analyze("delete from users as u where u.name='Trillian'");

        assertThat(actualStatement.analyzedRelation, equalTo(expectedStatement.analyzedRelation()));
        assertThat(actualStatement.whereClauses().get(0), equalTo(expectedStatement.whereClauses().get(0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereClauseObjectArrayField() throws Exception {
        analyze("delete from users where friends['id'] = 5");
    }

    @Test
    public void testBulkDelete() throws Exception {
        DeleteAnalyzedStatement analysis = analyze("delete from users where id = ?", new Object[][]{
                new Object[]{1},
                new Object[]{2},
                new Object[]{3},
                new Object[]{4},
        });
        assertThat(analysis.whereClauses().size(), is(4));
    }
}
