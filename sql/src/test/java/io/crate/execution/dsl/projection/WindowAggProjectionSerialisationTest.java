/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dsl.projection;

import com.google.inject.Guice;
import io.crate.analyze.WindowDefinition;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class WindowAggProjectionSerialisationTest {

    @Test
    public void testWindowAggProjectionSerialisation() throws IOException {
        FunctionImplementation sumFunctionImpl = getSumFunction();

        WindowDefinition partitionByOneWindowDef =
            new WindowDefinition(singletonList(Literal.of(1L)), null, null);
        WindowDefinition partitionByTwoWindowDef =
            new WindowDefinition(singletonList(Literal.of(2L)), null, null);

        WindowFunction firstWindowFunction = new WindowFunction(sumFunctionImpl.info(), Arrays.asList(Literal.of(1L)), partitionByOneWindowDef);
        WindowFunction secondWindowFunction = new WindowFunction(sumFunctionImpl.info(), Arrays.asList(Literal.of(2L)), partitionByTwoWindowDef);

        LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs = new LinkedHashMap<>(2, 1f);
        functionsWithInputs.put(firstWindowFunction, Arrays.asList(Literal.of(1L)));
        functionsWithInputs.put(secondWindowFunction, Arrays.asList(Literal.of(2L)));

        WindowAggProjection windowAggProjection =
            new WindowAggProjection(partitionByOneWindowDef,
                functionsWithInputs,
                Collections.singletonList(Literal.of(42L)),
                new int[0]);
        BytesStreamOutput output = new BytesStreamOutput();
        windowAggProjection.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        WindowAggProjection fromInput = new WindowAggProjection(input);

        Map<WindowFunction, List<Symbol>> deserialisedFunctionsByWindow = fromInput.functionsWithInputs();

        assertThat(deserialisedFunctionsByWindow, equalTo(functionsWithInputs));
    }

    private FunctionImplementation getSumFunction() {
        Functions functions = Guice.createInjector(List.of(new AggregationImplModule(), new OperatorModule()))
            .getInstance(Functions.class);
        return functions.getQualified(new FunctionIdent(SumAggregation.NAME, Arrays.asList(DataTypes.FLOAT)));
    }
}
