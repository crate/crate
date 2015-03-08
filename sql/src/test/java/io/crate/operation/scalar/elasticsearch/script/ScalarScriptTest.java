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

package io.crate.operation.scalar.elasticsearch.script;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.script.ScriptException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.crate.operation.scalar.elasticsearch.script.AbstractScalarScriptFactory.ScalarArgument;
import static io.crate.operation.scalar.elasticsearch.script.AbstractScalarSearchScriptFactory.SearchContext;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ScalarScriptTest extends CrateUnitTest {

    private Functions functions;

    @Before
    public void prepare() {
        ModulesBuilder builder = new ModulesBuilder();
        builder.add(
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule());
        Injector injector = builder.createInjector();
        functions = injector.getInstance(Functions.class);
    }

    private SearchContext prepareScriptParams(Map<String, Object> params) {
        NumericScalarSearchScript.Factory factory = new NumericScalarSearchScript.Factory(functions);
        return (SearchContext) factory.prepare(params);
    }

    @Test
    public void testPrepareScalarEqualsLiteral() throws Exception {
        Map<String, Object> params = new MapBuilder<String, Object>()
                .put("op", EqOperator.NAME)
                .put("args", ImmutableList.of(
                        new MapBuilder<String, Object>()
                                .put("scalar_name", "round")
                                .put("args", ImmutableList.of(
                                        new MapBuilder<String, Object>()
                                                .put("field_name", "bla")
                                                .put("type", DataTypes.DOUBLE.id())
                                                .map()
                                ))
                                .put("type", DataTypes.DOUBLE.id())
                                .map(),
                        new MapBuilder<String, Object>()
                                .put("value", 12.0)
                                .put("type", DataTypes.DOUBLE.id())
                                .map()
                ))
                .map();

        SearchContext ctx = prepareScriptParams(params);

        assertThat(ctx.operator.info().ident().name(), is("op_="));
        assertThat(ctx.operatorArgs.get(1), instanceOf(AbstractScalarScriptFactory.LiteralArgument.class));
        assertThat(ctx.operatorArgs.get(1).getType(), is((DataType)DataTypes.DOUBLE));
        assertThat(
                ((AbstractScalarScriptFactory.LiteralArgument)ctx.operatorArgs.get(1)).value.value(),
                is((Object)12.0));
        assertThat(ctx.operatorArgs.get(0),
                is((AbstractScalarScriptFactory.WrappedArgument)ctx.function));
        assertThat(ctx.operatorArgs.get(0).getType(), is((DataType)DataTypes.DOUBLE));
        assertThat(((ScalarArgument)ctx.operatorArgs.get(0)).args.get(0),
                is((AbstractScalarScriptFactory.WrappedArgument)new AbstractScalarScriptFactory.ReferenceArgument("bla", DataTypes.DOUBLE)));
    }

    @Test
    public void testPrepareScalarOnRight() throws Exception {
        expectedException.expect(ScriptException.class);
        expectedException.expectMessage("first argument in search script no scalar!");

        Map<String, Object> params = new MapBuilder<String, Object>()
                .put("op", EqOperator.NAME)
                .put("args", ImmutableList.of(
                        new MapBuilder<String, Object>()
                                .put("value", 12.0)
                                .put("type", DataTypes.DOUBLE.id())
                                .map(),
                        new MapBuilder<String, Object>()
                                .put("scalar_name", "round")
                                .put("args", ImmutableList.of(
                                        new MapBuilder<String, Object>()
                                                .put("field_name", "bla")
                                                .put("type", DataTypes.DOUBLE.id())
                                                .map()
                                ))
                                .put("type", DataTypes.DOUBLE.id())
                                .map()
                ))
                .map();

        prepareScriptParams(params);
    }

    @Test
    public void testPrepareFloatArgumentsTurnedToDouble() throws Exception {
        Map<String, Object> params = new MapBuilder<String, Object>()
                .put("op", EqOperator.NAME)
                .put("args", ImmutableList.of(
                        new MapBuilder<String, Object>()
                                .put("scalar_name", "round")
                                .put("args", ImmutableList.of(
                                        new MapBuilder<String, Object>()
                                                .put("field_name", "bla")
                                                .put("type", DataTypes.FLOAT.id())
                                                .map()
                                ))
                                .put("type", DataTypes.INTEGER.id())
                                .map(),
                        new MapBuilder<String, Object>()
                                .put("value", 12.0)
                                .put("type", DataTypes.FLOAT.id())
                                .map()
                ))
                .map();

        SearchContext ctx = prepareScriptParams(params);
        assertThat(ctx.operatorArgs.size(), is(2));
        AbstractScalarScriptFactory.WrappedArgument arg1 = ctx.operatorArgs.get(0);
        assertThat(arg1, instanceOf(ScalarArgument.class));
        ScalarArgument scalarArg1 = (ScalarArgument) arg1;
        assertThat(scalarArg1.getType(), is((DataType)DataTypes.LONG));
        assertThat(scalarArg1.args.get(0).getType(), is((DataType)DataTypes.DOUBLE));
    }
}
