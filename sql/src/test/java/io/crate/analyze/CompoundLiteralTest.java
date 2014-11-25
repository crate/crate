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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.*;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CompoundLiteralTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SelectStatementAnalyzer analyzer;

    @Before
    public void prepare() {
        analyzer = new SelectStatementAnalyzer(
                injector.getInstance(ReferenceInfos.class),
                injector.getInstance(Functions.class),
                injector.getInstance(ReferenceResolver.class)
        );

    }

    @Override
    protected List<Module> getModules() {
        return Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new MetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule());
    }

    public Symbol analyzeExpression(String expression) {
        return analyzeExpression(expression, new Object[0]);
    }

    public Symbol analyzeExpression(String expression, Object[] params) {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) analyzer.newAnalysis(
                new ParameterContext(params, new Object[0][]));
        return SqlParser.createExpression(expression).accept(analyzer, analysis);

    }

    @Test
    public void testObjectLiteral() throws Exception {
        Symbol s = analyzeExpression("{}");
        assertThat(s, instanceOf(Literal.class));
        Literal l = (Literal)s;
        assertThat(l.value(), is((Object)new HashMap<String, Object>()));

        Literal objectLiteral = (Literal)analyzeExpression("{ident='value'}");
        assertThat(objectLiteral.symbolType(), is(SymbolType.LITERAL));
        assertThat(objectLiteral.valueType(), is((DataType)ObjectType.INSTANCE));
        assertThat(objectLiteral.value(), is((Object) new MapBuilder<String, Object>().put("ident", "value").map()));

        Literal multipleObjectLiteral = (Literal)analyzeExpression("{\"Ident\"=123.4, a={}, ident='string'}");
        Map<String, Object> values = (Map<String, Object>)multipleObjectLiteral.value();
        assertThat(values, is(new MapBuilder<String, Object>()
                .put("Ident", 123.4d)
                .put("a", new HashMap<String, Object>())
                .put("ident", "string")
                .map()));
    }

    @Test
    public void testObjectliteralWithParameter() throws Exception {
        Literal objectLiteral = (Literal) analyzeExpression("{ident=?}", new Object[]{1});
        assertThat(objectLiteral.valueType(), is((DataType)ObjectType.INSTANCE));
        assertThat(objectLiteral.value(), is((Object) new MapBuilder<String, Object>().put("ident", 1).map()));
    }

    @Test
    public void testObjectLiteralWithFunction() throws Exception {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:4: no viable alternative at input 'format'");
        analyzeExpression("{a=format('%s.', 'dot')}");
    }

    @Test
    public void testObjectLiteralKeyTwice() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("key 'a' listed twice in object literal");
        analyzeExpression("{a=1, a=2}");
    }

    @Test
    public void testArrayLiteral() throws Exception {
        Literal emptyArray = (Literal) analyzeExpression("[]");
        assertThat((Object[])emptyArray.value(), is(new Object[0]));
        assertThat(emptyArray.valueType(), is((DataType)new ArrayType(UndefinedType.INSTANCE)));

        Literal singleArray = (Literal) analyzeExpression("[1]");
        assertThat(singleArray.valueType(), is((DataType)new ArrayType(LongType.INSTANCE)));
        assertThat(((Object[])singleArray.value()).length, is(1));
        assertThat(((Object[])singleArray.value())[0], is((Object)1L));

        Literal multiArray = (Literal) analyzeExpression("[1, 2, 3]");
        assertThat(multiArray.valueType(), is((DataType)new ArrayType(LongType.INSTANCE)));
        assertThat(((Object[])multiArray.value()).length, is(3));
        assertThat((Object[])multiArray.value(), is(new Object[]{1L,2L,3L}));
    }

    @Test
    public void testArrayLiteralWithParameter() throws Exception {
        Literal array = (Literal) analyzeExpression("[1, ?]", new Object[]{4L});
        assertThat(array.valueType(), is((DataType)new ArrayType(LongType.INSTANCE)));
        assertThat(((Object[])array.value()).length, is(2));
        assertThat((Object[])array.value(), is(new Object[]{1L, 4L}));
    }

    @Test
    public void testArrayDifferentTypes() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array element 'string' not of array item type long");
        analyzeExpression("[1, 'string']");
    }

    @Test
    public void testArrayDifferentTypesString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("array element 1 not of array item type string");
        analyzeExpression("['string', 1]");
    }

    @Test
    public void testNestedArrayLiteral() throws Exception {
        Map<String, DataType> expected = ImmutableMap.<String, DataType>builder()
                .put("'string'", DataTypes.STRING)
                .put("0", DataTypes.LONG)
                .put("1.8", DataTypes.DOUBLE)
                .put("TRUE", DataTypes.BOOLEAN)
                .build();
        for (Map.Entry<String, DataType> entry : expected.entrySet()) {
            Symbol nestedArraySymbol = analyzeExpression("[[" + entry.getKey() + "]]");
            assertThat(nestedArraySymbol, Matchers.instanceOf(Literal.class));
            Literal nestedArray = (Literal)nestedArraySymbol;
            assertThat(nestedArray.valueType(), is((DataType)new ArrayType(new ArrayType(entry.getValue()))));
        }
    }
}
