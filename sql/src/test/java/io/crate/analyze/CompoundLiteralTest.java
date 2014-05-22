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
import io.crate.sql.parser.SqlParser;
import io.crate.types.*;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CompoundLiteralTest extends BaseAnalyzerTest {

    @Override
    protected List<Module> getModules() {
        return Arrays.<Module>asList(
                new TestModule(),
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
        SelectAnalysis analysis = new SelectAnalysis(
                injector.getInstance(ReferenceInfos.class),
                injector.getInstance(Functions.class),
                params,
                injector.getInstance(ReferenceResolver.class)
        );
        SelectStatementAnalyzer analyzer = new SelectStatementAnalyzer();
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

    @Test(expected = IllegalArgumentException.class)
    public void testObjectLiteralWithFunction() throws Exception {
        analyzeExpression("{a=format('%s.', 'dot')}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testObjectLiteralKeyTwice() throws Exception {
        analyzeExpression("{a=1, a=2}");
    }

    @Test
    public void testArrayLiteral() throws Exception {
        Literal emptyArray = (Literal) analyzeExpression("[]");
        assertThat((Object[])emptyArray.value(), is(new Object[0]));
        assertThat(emptyArray.valueType(), is((DataType)new ArrayType(NullType.INSTANCE)));

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

    @Test(expected = IllegalArgumentException.class)
    public void testArrayDifferentTypes() throws Exception {
        analyzeExpression("[1, 'string']");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayDifferentTypesString() throws Exception {
        analyzeExpression("['string', 1]");
    }
}
