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
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operator.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalysisTest {

    private ReferenceInfos referenceInfos;
    private ReferenceResolver resolver;
    private Functions functions;
    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(null, "test1");
    private static final FunctionInfo TEST_FUNCTION_INFO = new FunctionInfo(new FunctionIdent("abs", ImmutableList.of(DataType.DOUBLE)), DataType.DOUBLE);
    private static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataType.LONG, null)
            .add("name", DataType.STRING, null)
            .add("d", DataType.DOUBLE, null)
            .add("dyn_empty", DataType.OBJECT, null, ReferenceInfo.ObjectType.DYNAMIC)
            .add("dyn", DataType.OBJECT, null, ReferenceInfo.ObjectType.DYNAMIC)
            .add("dyn", DataType.DOUBLE, ImmutableList.of("d"))
            .add("dyn", DataType.OBJECT, ImmutableList.of("inner_strict"), ReferenceInfo.ObjectType.STRICT)
            .add("dyn", DataType.DOUBLE, ImmutableList.of("inner_strict", "double"))
            .add("strict", DataType.OBJECT, null, ReferenceInfo.ObjectType.STRICT)
            .add("strict", DataType.DOUBLE, ImmutableList.of("inner_d"))
            .add("ignored", DataType.OBJECT, null, ReferenceInfo.ObjectType.IGNORED)
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();


    static class AbsFunction implements Scalar<Double> {

        @Override
        public Double evaluate(Input<?>... args) {
            if (args == null || args.length == 0) {
                return 0.0d;
            }
            return Math.abs(((Number)args[0].value()).doubleValue());
        }

        @Override
        public FunctionInfo info() {
            return TEST_FUNCTION_INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            if (symbol.arguments().size() == 1 && symbol.arguments().get(0).symbolType() == SymbolType.DOUBLE_LITERAL) {
                return new DoubleLiteral(evaluate(((Input)symbol.arguments().get(0))));
            }
            return symbol;
        }
    }

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(TEST_FUNCTION_INFO.ident()).toInstance(new AbsFunction());
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }



    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new TestMetaDataModule())
                .createInjector();
        referenceInfos = injector.getInstance(ReferenceInfos.class);
        resolver = injector.getInstance(ReferenceResolver.class);
        functions = injector.getInstance(Functions.class);
    }

    public Analysis getAnalysis() {
        return getAnalysis(new Object[0]);
    }

    public Analysis getAnalysis(Object[] args) {
        SelectAnalysis analysis = new SelectAnalysis(referenceInfos, functions, args, resolver);
        analysis.table(TEST_TABLE_IDENT);
        return analysis;
    }

    @Test
    public void testNormalizePrimitiveLiteral() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo info = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.BOOLEAN)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("bool")).build();
        BooleanLiteral trueLiteral = new BooleanLiteral(true);
        assertThat(analysis.normalizeInputValue(trueLiteral, new Reference(info)), Matchers.<Literal>is(trueLiteral));

        assertThat(analysis.normalizeInputValue(new StringLiteral("true"), new Reference(info)), Matchers.<Literal>is(trueLiteral));
        assertThat(analysis.normalizeInputValue(new StringLiteral("false"), new Reference(info)), Matchers.<Literal>is(new BooleanLiteral(false)));
    }

    @Test
    public void testNormalizeScalar() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo info = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("double")).build();
        Function f = new Function(TEST_FUNCTION_INFO, Arrays.<Symbol>asList(new DoubleLiteral(-9.9)));
        assertThat(analysis.normalizeInputValue(f, new Reference(info)), Matchers.<Literal>is(new DoubleLiteral(9.9)));
    }

    @Test
    public void testNormalizeDynamicEmptyObjectLiteral() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn_empty"));
        Map<String, Object> map = new HashMap<>();
        map.put("time", "2014-02-16T00:00:01");
        map.put("false", true);
        ObjectLiteral normalized = (ObjectLiteral)analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        // TODO: guess and map unknown references
        assertThat((String)normalized.value().get("time"), is("2014-02-16T00:00:01"));
        assertThat((Boolean)normalized.value().get("false"), is(true));
    }

    @Test( expected = ValidationException.class)
    public void testNormalizeObjectLiteralInvalidNested() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2014-02-16T00:00:01");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }

    @Test
    public void testNormalizeObjectLiteralConvertFromString() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2.9");

        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized, Matchers.instanceOf(ObjectLiteral.class));
        assertThat(((ObjectLiteral)normalized).value().get("d"), Matchers.<Object>is(2.9d));
    }

    @Test
    public void testNormalizeObjectLiteral() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("d", 2.9d);
            put("inner_strict", new HashMap<String, Object>(){{
                put("double", "-88.7");
            }});
        }};
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));

        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized, Matchers.instanceOf(ObjectLiteral.class));
        assertThat(((ObjectLiteral)normalized).value().get("d"), Matchers.<Object>is(2.9d));
        assertThat(((ObjectLiteral)normalized).value().get("inner_strict"),
                Matchers.<Object>is(new HashMap<String, Object>(){{
                    put("double", -88.7d);
                }}
        ));
    }

    @Test
    public void testNormalizeDynamicObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", 2.9d);
        map.put("half", "1.45");
        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized.value(), Matchers.<Object>is(map)); // stays the same
    }

    @Test(expected = ValidationException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("half", "1.45");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }

    @Test(expected = ValidationException.class)
    public void testNormalizeNestedStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo objInfo = userTableInfo.getColumnInfo(new ColumnIdent("dyn"));

        Map<String, Object> map = new HashMap<>();
        map.put("inner_strict", new HashMap<String, Object>(){{
            put("double", 2.9d);
            put("half", "1.45");
        }});
        map.put("half", "1.45");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }


}
