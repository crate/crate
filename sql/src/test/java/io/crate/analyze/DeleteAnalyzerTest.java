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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.PartitionName;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
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
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    protected DeleteAnalyzedStatement analyze(String statement, Object[][] bulkArgs) {
        return (DeleteAnalyzedStatement) super.analyze(statement, bulkArgs);
    }

    protected DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement analyze(String statement) {
        return ((DeleteAnalyzedStatement) super.analyze(statement)).nestedStatements().get(0);
    }

    @Test
    public void testDeleteWhere() throws Exception {
        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement analysis = analyze("delete from users where name='Trillian'");
        assertEquals(TEST_DOC_TABLE_IDENT, analysis.table().ident());

        assertThat(analysis.table().rowGranularity(), is(RowGranularity.DOC));

        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));

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
        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement analysis = analyze("delete from parted where date = 1395874800000");
        assertThat(analysis.whereClause().hasQuery(), Matchers.is(false));
        assertThat(analysis.whereClause().noMatch(), Matchers.is(false));
        assertEquals(ImmutableList.of(
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()),
                analysis.whereClause().partitions());

        analysis = analyze("delete from parted");
        assertThat(analysis.whereClause().hasQuery(), Matchers.is(false));
        assertThat(analysis.whereClause().noMatch(), Matchers.is(false));
        assertEquals(ImmutableList.<String>of(), analysis.whereClause().partitions());
    }

    @Test
    public void testDeleteTableAlias() throws Exception {
        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement expectedAnalysis = analyze(
                "delete from users where name='Trillian'");
        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement actualAnalysis = analyze(
                "delete from users as u where u.name='Trillian'");

        assertEquals(actualAnalysis.tableAlias(), "u");
        assertEquals(
                ((Function)expectedAnalysis.whereClause().query()).arguments().get(0),
                ((Function)actualAnalysis.whereClause().query()).arguments().get(0)
        );
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
        assertThat(analysis.nestedStatements().size(), is(4));

        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement firstAnalysis = analysis.nestedStatements().get(0);
        assertThat(firstAnalysis.ids().get(0), is("1"));
        DeleteAnalyzedStatement.NestedDeleteAnalyzedStatement secondAnalysis = analysis.nestedStatements().get(1);
        assertThat(secondAnalysis.ids().get(0), is("2"));
    }
}
