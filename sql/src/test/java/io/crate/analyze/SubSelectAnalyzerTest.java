/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isField;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubSelectAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo docSchemaInfo = mock(SchemaInfo.class);
            when(docSchemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            when(docSchemaInfo.getTableInfo(USER_TABLE_IDENT_MULTI_PK.name())).thenReturn(USER_TABLE_INFO_MULTI_PK);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(docSchemaInfo);
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            new TestMetaDataModule(),
            new ScalarFunctionModule()
        ));
        return modules;
    }

    @Test
    public void testSimpleSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select ausers.id / ausers.other_id from (select id, other_id from users) as ausers");
        QueriedSelectRelation relation = (QueriedSelectRelation) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("(ausers.id / ausers.other_id)"));

        QueriedDocTable docTable = (QueriedDocTable) relation.relation();
        assertThat(docTable.tableRelation().tableInfo(), is(USER_TABLE_INFO));
    }

    @Test
    public void testSimpleSubSelectWithMixedCases() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select ausers.ID from (select id from users) as AUSERS");
        QueriedSelectRelation relation = (QueriedSelectRelation) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("ausers.id"));
    }

    @Test
    public void testSubSelectWithoutAlias() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("subquery in FROM must have an alias");
        analyze("select aid from (select id as aid from users)");
    }

    @Test
    public void testSubSelectWithNestedAlias() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select ausers.aid from (select id as aid from users) as ausers");
        QueriedSelectRelation relation = (QueriedSelectRelation) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("ausers.aid"));

        QueriedDocTable docTable = (QueriedDocTable) relation.relation();
        assertThat(docTable.tableRelation().tableInfo(), is(USER_TABLE_INFO));
    }
}
