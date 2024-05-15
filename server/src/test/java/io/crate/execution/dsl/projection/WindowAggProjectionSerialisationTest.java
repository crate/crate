/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dsl.projection;

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.singletonList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.analyze.WindowDefinition;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;

public class WindowAggProjectionSerialisationTest {

    private Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));

    @Test
    public void testWindowAggProjectionSerialisation() throws IOException {
        FunctionImplementation sumFunctionImpl = getSumFunction();

        WindowDefinition partitionByOneWindowDef =
            new WindowDefinition(singletonList(Literal.of(1L)), null, null);
        WindowDefinition partitionByTwoWindowDef =
            new WindowDefinition(singletonList(Literal.of(2L)), null, null);

        WindowFunction firstWindowFunction = new WindowFunction(
            sumFunctionImpl.signature(),
            singletonList(Literal.of(1L)),
            sumFunctionImpl.boundSignature().returnType(),
            null,
            partitionByOneWindowDef,
            true
        );
        WindowFunction secondWindowFunction = new WindowFunction(
            sumFunctionImpl.signature(),
            singletonList(Literal.of(2L)),
            sumFunctionImpl.boundSignature().returnType(),
            null,
            partitionByTwoWindowDef,
            null
        );

        Symbol standaloneInput = Literal.of(42L);
        var expectedWindowAggProjection = new WindowAggProjection(
            partitionByOneWindowDef,
            List.of(firstWindowFunction, secondWindowFunction),
            List.of(standaloneInput));

        var output = new BytesStreamOutput();
        expectedWindowAggProjection.writeTo(output);

        var in = output.bytes().streamInput();
        var actualWindowAggProjection = new WindowAggProjection(in);

        assertThat(
            actualWindowAggProjection.outputs(),
            contains(standaloneInput, firstWindowFunction, secondWindowFunction));
        assertThat(actualWindowAggProjection).isEqualTo(expectedWindowAggProjection);
    }

    @Test
    public void test_window_agg_projection_serialization_with_filter_before_4_1_0()
        throws IOException {
        FunctionImplementation sumFunctionImpl = getSumFunction();

        WindowDefinition partitionByOneWindowDef =
            new WindowDefinition(singletonList(Literal.of(1L)), null, null);

        WindowFunction windowFunction = new WindowFunction(
            sumFunctionImpl.signature(),
            singletonList(Literal.of(2L)),
            sumFunctionImpl.boundSignature().returnType(),
            null,
            partitionByOneWindowDef,
            null);

        Symbol standaloneInput = Literal.of(42L);
        var windowAggProjection = new WindowAggProjection(
            partitionByOneWindowDef,
            List.of(windowFunction),
            List.of(standaloneInput));

        var output = new BytesStreamOutput();
        output.setVersion(Version.V_4_0_0);
        windowAggProjection.writeTo(output);

        var input = output.bytes().streamInput();
        input.setVersion(Version.V_4_0_0);
        var actualWindowAggProjection = new WindowAggProjection(input);

        assertThat(
            actualWindowAggProjection.outputs(),
            contains(standaloneInput, windowFunction));
        assertThat(actualWindowAggProjection.windowFunctions().get(0).filter()).isNull();
    }

    private FunctionImplementation getSumFunction() {
        return functions.getQualified(
            Signature.aggregate(
                SumAggregation.NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()),
            List.of(DataTypes.LONG),
            DataTypes.LONG
        );
    }
}
