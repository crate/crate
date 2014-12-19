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

import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropTableAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new MetaDataInformationModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    @Test( expected = TableUnknownException.class )
    public void testDropNonExistingTable() throws Exception {
        analyze("drop table unknown");
    }

    @Test( expected = UnsupportedOperationException.class)
    public void testDropSystemTable() throws Exception {
        analyze("drop table sys.cluster");
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testDropInformationSchemaTable() throws Exception {
        analyze("drop table information_schema.tables");
    }

    @Test( expected = SchemaUnknownException.class )
    public void testDropUnknownSchema() throws Exception {
        analyze("drop table unknown_schema.unknown");
    }

    @Test
    public void testDropExistingTable() throws Exception {
        AnalyzedStatement analyzedStatement = analyze(String.format(Locale.ENGLISH, "drop table %s", TEST_DOC_TABLE_IDENT.name()));
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;

        assertThat(dropTableAnalysis.index(), is(TEST_DOC_TABLE_IDENT.name()));
    }
}
