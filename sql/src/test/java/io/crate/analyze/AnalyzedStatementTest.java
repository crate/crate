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
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AnalyzedStatementTest {

    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(null, "test1");
    private static final FunctionInfo TEST_FUNCTION_INFO = new FunctionInfo(
            new FunctionIdent("abs", ImmutableList.<DataType>of(DataTypes.DOUBLE)), DataTypes.DOUBLE);
    private static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("d", DataTypes.DOUBLE, null)
            .add("dyn_empty", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("dyn", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("dyn", DataTypes.DOUBLE, ImmutableList.of("d"))
            .add("dyn", DataTypes.OBJECT, ImmutableList.of("inner_strict"), ColumnPolicy.STRICT)
            .add("dyn", DataTypes.DOUBLE, ImmutableList.of("inner_strict", "double"))
            .add("strict", DataTypes.OBJECT, null, ColumnPolicy.STRICT)
            .add("strict", DataTypes.DOUBLE, ImmutableList.of("inner_d"))
            .add("ignored", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();

    private ExpressionAnalyzer expressionAnalyzer;


    static class AbsFunction extends Scalar<Double, Number> {

        @Override
        public Double evaluate(Input<Number>... args) {
            if (args == null || args.length == 0) {
                return 0.0d;
            }
            return Math.abs((args[0].value()).doubleValue());
        }

        @Override
        public FunctionInfo info() {
            return TEST_FUNCTION_INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            if (symbol.arguments().size() == 1
                    && symbol.arguments().get(0).symbolType() == SymbolType.LITERAL
                    && symbol.arguments().get(0).valueType().equals(DataTypes.DOUBLE)) {
                return Literal.newLiteral(evaluate(((Input)symbol.arguments().get(0))));
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
        expressionAnalyzer = new ExpressionAnalyzer(injector.getInstance(AnalysisMetaData.class),
                new ParameterContext(new Object[0], new Object[0][]),
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                        new QualifiedName(Arrays.asList("doc", "test1")), new TableRelation(userTableInfo))
        );
    }

    @Test
    public void testNormalizePrimitiveLiteral() throws Exception {
        ReferenceInfo info = new ReferenceInfo.Builder()
                .granularity(RowGranularity.DOC)
                .type(DataTypes.BOOLEAN)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("bool")).build();
        Literal<Boolean> trueLiteral = Literal.newLiteral(true);

        assertThat(expressionAnalyzer.normalizeInputForReference(trueLiteral, new Reference(info)),
                Matchers.<Literal>is(trueLiteral));

        Reference infoRef = new Reference(info);
        assertThat(expressionAnalyzer.normalizeInputForReference(Literal.newLiteral("true"), infoRef),
                Matchers.<Literal>is(trueLiteral));
        assertThat(expressionAnalyzer.normalizeInputForReference(Literal.newLiteral("false"), infoRef),
                Matchers.<Literal>is(Literal.newLiteral(false)));
    }

    @Test
    public void testNormalizeScalar() throws Exception {
        ReferenceInfo info = new ReferenceInfo.Builder()
                .granularity(RowGranularity.DOC)
                .type(DataTypes.DOUBLE)
                .ident(TEST_TABLE_IDENT, new ColumnIdent("double")).build();
        Function f = new Function(TEST_FUNCTION_INFO, Arrays.<Symbol>asList(Literal.newLiteral(-9.9)));
        assertThat(expressionAnalyzer.normalizeInputForReference(f, new Reference(info)), Matchers.<Literal>is(Literal.newLiteral(9.9)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeDynamicEmptyObjectLiteral() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn_empty"));
        Map<String, Object> map = new HashMap<>();
        map.put("time", "2014-02-16T00:00:01");
        map.put("false", true);
        Literal<Map<String, Object>> normalized = expressionAnalyzer.normalizeInputForReference(
                Literal.newLiteral(map), new Reference(objInfo));
        assertThat((Long) normalized.value().get("time"), is(1392508801000l));
        assertThat((Boolean)normalized.value().get("false"), is(true));
    }

    @Test( expected = ColumnValidationException.class)
    public void testNormalizeObjectLiteralInvalidNested() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2014-02-16T00:00:01");
        expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
    }

    @Test
    public void testNormalizeObjectLiteralConvertFromString() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2.9");

        Literal normalized = expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(((Literal<Map<String, Object>>)normalized).value().get("d"), Matchers.<Object>is(2.9d));
    }

    @Test
    public void testNormalizeObjectLiteral() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("d", 2.9d);
            put("inner_strict", new HashMap<String, Object>(){{
                put("double", "-88.7");
            }});
        }};
        expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));

        Literal normalized = expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(((Literal<Map<String, Object>>)normalized).value().get("d"), Matchers.<Object>is(2.9d));
        assertThat(((Literal<Map<String, Object>>)normalized).value().get("inner_strict"),
                Matchers.<Object>is(new HashMap<String, Object>(){{
                    put("double", -88.7d);
                }}
        ));
    }

    @Test
    public void testNormalizeDynamicObjectLiteralWithAdditionalColumn() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", 2.9d);
        map.put("half", "1.45");
        Literal normalized = expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
        assertThat(normalized.value(), Matchers.<Object>is(map)); // stays the same
    }


    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalColumn() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("half", "1.45");
        expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
    }

    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalNestedColumn() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("inner_map", new HashMap<String, Object>(){{
            put("much_inner", "yaw");
        }});
       expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
    }

    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeNestedStrictObjectLiteralWithAdditionalColumn() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));

        Map<String, Object> map = new HashMap<>();
        map.put("inner_strict", new HashMap<String, Object>(){{
            put("double", 2.9d);
            put("half", "1.45");
        }});
        map.put("half", "1.45");
        expressionAnalyzer.normalizeInputForReference(Literal.newLiteral(map), new Reference(objInfo));
    }

    @Test
    public void testNormalizeDynamicNewColumnTimestamp() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("time", "1970-01-01T00:00:00");
        }};
        Literal<Map<String, Object>> literal = expressionAnalyzer.normalizeInputForReference(
                Literal.newLiteral(map),
                new Reference(objInfo)
        );
        assertThat((Long) literal.value().get("time"), is(0l));
    }

    @Test
    public void testNormalizeIgnoredNewColumnTimestamp() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("ignored"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("time", "1970-01-01T00:00:00");
        }};
        Literal<Map<String, Object>> literal = expressionAnalyzer.normalizeInputForReference(
                Literal.newLiteral(map),
                new Reference(objInfo)
        );
        assertThat((String)literal.value().get("time"), is("1970-01-01T00:00:00"));
    }

    @Test
    public void testNormalizeDynamicNewColumnNoTimestamp() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("ignored"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("no_time", "1970");
        }};
        Literal<Map<String, Object>> literal = expressionAnalyzer.normalizeInputForReference(
                Literal.newLiteral(map),
                new Reference(objInfo)
        );
        assertThat((String)literal.value().get("no_time"), is("1970"));
    }

    @Test
    public void testNormalizeStringToNumberColumn() throws Exception {
        ReferenceInfo objInfo = userTableInfo.getReferenceInfo(new ColumnIdent("d"));
        Literal<BytesRef> stringDoubleLiteral = Literal.newLiteral("298.444");
        Literal literal = expressionAnalyzer.normalizeInputForReference(stringDoubleLiteral, new Reference(objInfo));
        assertLiteralSymbol(literal, 298.444d);
    }
}
