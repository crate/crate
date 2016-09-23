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

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isField;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExplainAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            new TestMetaDataModule(),
            new MetaDataSysModule()
        ));
        return modules;
    }

    @Test
    public void testExplain() throws Exception {
        ExplainAnalyzedStatement stmt = analyze("explain select id from sys.cluster");
        assertNotNull(stmt.statement());
        assertThat(stmt.statement(), instanceOf(SelectAnalyzedStatement.class));
    }

    @Test
    public void testExplainCopyFrom() throws Exception {
        ExplainAnalyzedStatement stmt = analyze("explain copy users from '/tmp/*' WITH (shared=True)");
        assertThat(stmt.statement(), instanceOf(CopyFromAnalyzedStatement.class));
        assertThat(stmt.fields(), Matchers.contains(isField("EXPLAIN COPY \"users\" FROM '/tmp/*' WITH (\n   shared = true\n)")));
    }

    @Test
    public void testExplainRefreshUnsupported() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("EXPLAIN is not supported for RefreshStatement");
        analyze("explain refresh table parted");
    }

    @Test
    public void testExplainOptimizeUnsupported() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("EXPLAIN is not supported for OptimizeStatement");
        analyze("explain optimize table parted");
    }

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo docSchemaInfo = mock(SchemaInfo.class);
            when(docSchemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(docSchemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(docSchemaInfo);
        }
    }
}
