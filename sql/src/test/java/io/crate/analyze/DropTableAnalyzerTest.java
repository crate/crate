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
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropTableAnalyzerTest extends BaseAnalyzerTest {

    private static final TableIdent ALIAS_IDENT = new TableIdent(DocSchemaInfo.NAME, "alias_table");
    private static final TableInfo ALIAS_INFO = TestingTableInfo.builder(ALIAS_IDENT, SHARD_ROUTING)
            .add("col", DataTypes.STRING, ImmutableList.<String>of())
            .isAlias(true)
            .build();

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            when(schemaInfo.getTableInfo(ALIAS_IDENT.name())).thenReturn(ALIAS_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
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

    @Test
    public void testDropNonExistingTable() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.unknown' unknown");
        analyze("drop table unknown");
    }

    @Test
    public void testDropSystemTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The table sys.cluster is not dropable.");
        analyze("drop table sys.cluster");
    }

    @Test
    public void testDropInformationSchemaTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The table information_schema.tables is not dropable.");
        analyze("drop table information_schema.tables");
    }

    @Test
    public void testDropUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'unknown_schema' unknown");
        analyze("drop table unknown_schema.unknown");
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        // shouldn't raise SchemaUnknownException / TableUnknownException
        analyze("drop table if exists unknown_schema.unknown");
    }

    @Test
    public void testDropExistingTable() throws Exception {
        AnalyzedStatement analyzedStatement = analyze(format(ENGLISH, "drop table %s", USER_TABLE_IDENT.name()));
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(false));
        assertThat(dropTableAnalysis.index(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testDropIfExistExistingTable() throws Exception {
        AnalyzedStatement analyzedStatement = analyze(format(ENGLISH, "drop table if exists %s", USER_TABLE_IDENT.name()));
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(true));
        assertThat(dropTableAnalysis.index(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testNonExistentTableIsRecognizedCorrectly() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("drop table if exists unknowntable");
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(true));
    }

    @Test
    public void testDropAliasFails() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("doc.alias_table is an alias and hence not dropable.");
        analyze("drop table alias_table");
    }

    @Test
    public void testDropAliasIfExists() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("doc.alias_table is an alias and hence not dropable.");
        analyze("drop table if exists alias_table");
    }
}
