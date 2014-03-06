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
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operator.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.DataType;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.ValidationException;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateAnalyzerTest extends BaseAnalyzerTest {

    private static TableIdent TEST_ALIAS_TABLE_IDENT = new TableIdent(null, "alias");
    private static TableInfo TEST_ALIAS_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_ALIAS_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("bla", DataType.STRING, null)
            .isAlias(true).build();

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_ALIAS_TABLE_IDENT.name())).thenReturn(TEST_ALIAS_TABLE_INFO);
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
                new OperatorModule(),
                new MetaDataSysModule()
        ));
        return modules;
    }

    @Test
    public void testUpdateAnalysis() throws Exception {
        Analysis analysis = analyze("update users set name='Ford Prefect'");
        assertThat(analysis, instanceOf(UpdateAnalysis.class));
    }

    @Test( expected = TableUnknownException.class)
    public void testUpdateUnknownTable() throws Exception {
        analyze("update unknown set name='Prosser'");
    }

    @Test( expected = ValidationException.class)
    public void testUpdateSetColumnToColumnValue() throws Exception {
        analyze("update users set name=name");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateSameReferenceRepeated() throws Exception {
        analyze("update users set name='Trillian', name='Ford'");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateSameNestedReferenceRepeated() throws Exception {
        analyze("update users set details['arms']=3, details['arms']=5");
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testUpdateSysTables() throws Exception {
        analyze("update sys.nodes set fs=?", new Object[]{new HashMap<String, Object>(){{
            put("free", 0);
        }}});
    }

    @Test
    public void testUpdateAssignments() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set name='Trillian'");
        assertThat(analysis.assignments().size(), is(1));
        assertThat(analysis.table().ident(), is(new TableIdent(null, "users")));

        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref.info().ident().tableIdent().name(), is("users"));
        assertThat(ref.info().ident().columnIdent().name(), is("name"));
        assertTrue(analysis.assignments().containsKey(ref));

        Symbol value = analysis.assignments().entrySet().iterator().next().getValue();
        assertThat(value, instanceOf(StringLiteral.class));
        assertThat(((StringLiteral)value).value().utf8ToString(), is("Trillian"));
    }

    @Test
    public void testUpdateAssignmentNestedDynamicColumn() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set details['arms']=3");
        assertThat(analysis.assignments().size(), is(1));

        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref, instanceOf(DynamicReference.class));
        assertThat(ref.info().type(), is(DataType.LONG));
        assertThat(ref.info().ident().columnIdent().isColumn(), is(false));
        assertThat(ref.info().ident().columnIdent().fqn(), is("details.arms"));
    }

    @Test( expected = ValidationException.class)
    public void testUpdateAssignmentWrongType() throws Exception {
        analyze("update users set id='String'");
    }

    @Test
    public void testUpdateAssignmentConvertableType() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set id=9.9");
        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref, not(instanceOf(DynamicReference.class)));
        assertThat(ref.info().type(), is(DataType.LONG));

        Symbol value = analysis.assignments().entrySet().iterator().next().getValue();
        assertThat(value, instanceOf(LongLiteral.class));
        assertThat(((LongLiteral)value).value(), is(9l));
    }

    @Test
    public void testUpdateMuchAssignments() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze(
                "update users set id=9.9, name='Trillian', details=?, stuff=true, foo='bar'",
                new Object[]{new HashMap<String, Object>()});
        assertThat(analysis.assignments().size(), is(5));
    }

    @Test
    public void testNoWhereClause() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set id=9");
        assertThat(analysis.whereClause(), is(WhereClause.MATCH_ALL));
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set id=9 where true=false");
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateWhereClause() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis) analyze("update users set id=9 where name='Trillian'");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        assertThat(analysis.whereClause().noMatch(), is(false));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrongQualifiedNameReferenceWithSchema() throws Exception {
        analyze("update users set sys.nodes.fs=?", new Object[]{new HashMap<String, Object>()});
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrongQualifiedNameReference() throws Exception {
        analyze("update users set unknown.name='Trillian'");
    }

    @Test
    public void testQualifiedNameReference() throws Exception {
        UpdateAnalysis analysis = (UpdateAnalysis)analyze("update users set users.name='Trillian'");
        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref.info().ident().tableIdent().name(), is("users"));
        assertThat(ref.info().ident().columnIdent().fqn(), is("name"));
    }
}
