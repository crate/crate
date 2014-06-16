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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlterTableAddColumnAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY.name()))
                    .thenReturn(userTableInfoClusteredByOnly);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.add(new TestModule());
        modules.add(new TestMetaDataModule());
        modules.add(new MetaDataSysModule());
        return modules;
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("tables of schema \"sys\" are read only.");
        analyze("alter table sys.shards add column foobar string");
    }

    @Test
    public void testAddColumnWithAnalyzerAndNonStringType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Can't use an Analyzer on column \"foobar['age']\" because analyzers are only allowed on columns of type \"string\"");
        analyze("alter table users add column foobar object as (age int index using fulltext)");
    }

    @Test
    public void testAddFulltextIndex() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Adding an index using ALTER TABLE ADD COLUMN is not supported");
        analyze("alter table users add column index ft_foo using fulltext (name)");
    }

    @Test
    public void testAddColumnThatExistsAlready() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The table \"users\" already has a column named \"name\"");
        analyze("alter table users add column name string");
    }

    @Test
    public void testAddColumnToATableWithoutPrimaryKey() throws Exception {
        AddColumnAnalysis analysis = (AddColumnAnalysis) analyze(
                "alter table users_clustered_by_only add column foobar string");
        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        Object primaryKeys = ((Map) mapping.get("_meta")).get("primary_keys");
        assertNull(primaryKeys); // _id shouldn't be included
    }

    @Test
    public void testAddColumnAsPrimaryKey() throws Exception {
        AddColumnAnalysis analysis = (AddColumnAnalysis) analyze(
                "alter table users add column additional_pk string primary key");

        assertThat(analysis.analyzedTableElements().primaryKeys(), Matchers.contains(
                "additional_pk", "id"
        ));

        AnalyzedColumnDefinition idColumn = null;
        AnalyzedColumnDefinition additionalPkColumn = null;
        for (AnalyzedColumnDefinition column : analysis.analyzedTableElements().columns()) {
            if (column.name().equals("id")) {
                idColumn = column;
            } else {
                additionalPkColumn = column;
            }
        }
        assertNotNull(idColumn);
        assertThat(idColumn.ident(), equalTo(new ColumnIdent("id")));
        assertThat(idColumn.dataType(), equalTo("long"));

        assertNotNull(additionalPkColumn);
        assertThat(additionalPkColumn.ident(), equalTo(new ColumnIdent("additional_pk")));
        assertThat(additionalPkColumn.dataType(), equalTo("string"));
    }

    @Test
    public void testAddPrimaryKeyColumnWithArrayTypeUnsupported() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use columns of type \"array\" as primary key");
        analyze("alter table users add column newpk array(string) primary key");
    }
}