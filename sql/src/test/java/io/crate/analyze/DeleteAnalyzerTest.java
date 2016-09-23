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

import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.exceptions.RelationUnknownException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isLiteral;
import static io.crate.testing.TestingHelpers.isReference;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            new TestMetaDataModule(),
            new MetaDataSysModule(),
            new PredicateModule(),
            new OperatorModule())
        );
        return modules;
    }

    @Override
    protected DeleteAnalyzedStatement analyze(String statement) {
        return (DeleteAnalyzedStatement) super.analyze(statement);
    }

    protected DeleteAnalyzedStatement analyze(String statement, Object[][] bulkArgs) {
        return (DeleteAnalyzedStatement) super.analyze(statement, bulkArgs);
    }

    @Test
    public void testDeleteWhere() throws Exception {
        DeleteAnalyzedStatement statement = analyze("delete from users where name='Trillian'");
        DocTableRelation tableRelation = statement.analyzedRelation;
        TableInfo tableInfo = tableRelation.tableInfo();
        assertThat(USER_TABLE_IDENT, equalTo(tableInfo.ident()));

        assertThat(tableInfo.rowGranularity(), is(RowGranularity.DOC));

        Function whereClause = (Function) statement.whereClauses.get(0).query();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        assertThat(whereClause.arguments().get(0), isReference("name"));
        assertThat(whereClause.arguments().get(1), isLiteral("Trillian"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteSystemTable() throws Exception {
        analyze("delete from sys.nodes where name='Trillian'");
    }

    @Test
    public void testDeleteWhereSysColumn() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'sys.nodes'");
        analyze("delete from users where sys.nodes.id = 'node_1'");
    }

    @Test
    public void testDeleteWherePartitionedByColumn() throws Exception {
        DeleteAnalyzedStatement statement = analyze("delete from parted where date = 1395874800000");
        assertThat(statement.whereClauses().get(0).hasQuery(), is(false));
        assertThat(statement.whereClauses().get(0).noMatch(), is(false));
        assertThat(statement.whereClauses().get(0).partitions().size(), is(1));
        assertThat(statement.whereClauses().get(0).partitions().get(0),
            is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
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

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("delete from users where _version is null",
            new Object[]{1});
    }


}
