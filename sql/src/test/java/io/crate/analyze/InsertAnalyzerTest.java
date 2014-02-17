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

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }

        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(ABS_FUNCTION_INFO.ident()).to(AbsFunction.class);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule()
        ));
        return modules;
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.STRING));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.LONG));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(StringLiteral.class));
        assertThat(values.get(1), instanceOf(LongLiteral.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertWithColumnsAndTooManyValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian', 2, true)");
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertWithColumnsAndTooLessValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian')");
    }

    @Test(expected = CrateException.class)
    public void testInsertWithWrongType() throws Exception {
        analyze("insert into users (name, id) values (1, 'Trillian')");
    }

    @Test(expected = CrateException.class)
    public void testInsertWithWrongParameterType() throws Exception {
        analyze("insert into users (name, id) values (?, ?)", new Object[]{1, true});
    }

    @Test
    public void testInsertWithConvertedTypes() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis)analyze("insert into users (id, name, awesome) values (?, 'Trillian', ?)", new Object[]{1.0f, "true"});

        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));
        assertThat(analysis.columns().get(2).valueType(), is(DataType.BOOLEAN));

        List<Symbol> valuesList = analysis.values().get(0);
        assertThat(valuesList.get(0), instanceOf(LongLiteral.class));
        assertThat(valuesList.get(2), instanceOf(BooleanLiteral.class));

    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (ABS(-1), 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(LongLiteral.class)); // normalized/evaluated
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertThat(analysis.columns().get(1).valueType(), is(DataType.STRING));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(2));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
        assertThat(values.get(1), instanceOf(StringLiteral.class));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertAnalysis analysis = (InsertAnalysis) analyze("insert into users values (1)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertThat(analysis.columns().get(0).valueType(), is(DataType.LONG));

        assertThat(analysis.values().size(), is(1));
        List<Symbol> values = analysis.values().get(0);
        assertThat(values.size(), is(1));
        assertThat(values.get(0), instanceOf(LongLiteral.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertIntoSysTable() throws Exception {
        analyze("insert into sys.nodes (id, name) values (666, 'evilNode')");
    }

    @Test(expected = CrateException.class)
    public void testInsertWithoutPrimaryKey() throws Exception {
        analyze("insert into users (name) values ('Trillian')");
    }
}
