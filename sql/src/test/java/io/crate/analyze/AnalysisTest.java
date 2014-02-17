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

public class AnalysisTest {

    private ReferenceInfos referenceInfos;
    private ReferenceResolver resolver;
    private Functions functions;
    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(null, "test1");
    private static final FunctionInfo TEST_FUNCTION_INFO = new FunctionInfo(new FunctionIdent("abs", ImmutableList.of(DataType.DOUBLE)), DataType.DOUBLE);

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
        return new SelectAnalysis(referenceInfos, functions, args, resolver);
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
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("time", "2014-02-16T00:00:01");
        map.put("false", true);
        assertThat(analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo)), Matchers.<Literal>is(new ObjectLiteral(map)));
    }

    @Test( expected = ValidationException.class)
    public void testNormalizeObjectLiteralInvalidNested() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("double", "2014-02-16T00:00:01");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }

    @Test
    public void testNormalizeObjectLiteralConvertFromString() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("double", "2.9");

        Map<String, Object> converted = new HashMap<>();
        map.put("double", 2.9d);
        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized, Matchers.instanceOf(ObjectLiteral.class));
        assertThat(((ObjectLiteral)normalized).value().get("double"), Matchers.<Object>is(2.9d));
    }

    @Test
    public void testNormalizeObjectLiteral() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("double", 2.9d);
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));

        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized, Matchers.instanceOf(ObjectLiteral.class));
        assertThat(((ObjectLiteral)normalized).value().get("double"), Matchers.<Object>is(2.9d));
    }

    @Test
    public void testNormalizeDynamicObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("double", 2.9d);
        map.put("half", "1.45");
        Literal normalized = analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
        assertThat(normalized.value(), Matchers.<Object>is(map)); // stays the same
    }

    @Test(expected = ValidationException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.STRICT)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();
        Map<String, Object> map = new HashMap<>();
        map.put("double", 2.9d);
        map.put("half", "1.45");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }

    @Test(expected = ValidationException.class)
    public void testNormalizeNestedStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Analysis analysis = getAnalysis();
        ReferenceInfo doubleInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj", Arrays.asList("double"))).build();
        ReferenceInfo objInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.STRICT)
                .addNestedColumn(doubleInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("obj")).build();

        ReferenceInfo outerInfo = ReferenceInfo.builder()
                .granularity(RowGranularity.DOC)
                .type(DataType.OBJECT)
                .objectType(ReferenceInfo.ObjectType.DYNAMIC)
                .addNestedColumn(objInfo)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("dyn")).build();

        Map<String, Object> map = new HashMap<>();
        map.put("obj", new HashMap<String, Object>(){{
            put("double", 2.9d);
            put("half", "1.45");
        }});
        map.put("half", "1.45");
        analysis.normalizeInputValue(new ObjectLiteral(map), new Reference(objInfo));
    }


}
