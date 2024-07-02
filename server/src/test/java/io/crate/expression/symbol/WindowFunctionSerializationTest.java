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

package io.crate.expression.symbol;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.analyze.WindowDefinition;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;

public class WindowFunctionSerializationTest {

    private Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));

    private FunctionImplementation dummyFunction =
        functions.getQualified(
            Signature.aggregate(
                    SumAggregation.NAME,
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature())
                .withFeature(Scalar.Feature.DETERMINISTIC),
            List.of(DataTypes.FLOAT),
            DataTypes.FLOAT
        );

    private WindowFunction windowFunctionWithIgnoreNullsSetToTrue =
        new WindowFunction(
            dummyFunction.signature(),
            singletonList(Literal.of(1L)),
            dummyFunction.boundSignature().returnType(),
            null,
            new WindowDefinition(singletonList(Literal.of(1L)), null, null),
            true
        );

    private WindowFunction windowFunctionWithIgnoreNullsSetToNull =
        new WindowFunction(
            dummyFunction.signature(),
            singletonList(Literal.of(1L)),
            dummyFunction.boundSignature().returnType(),
            null,
            new WindowDefinition(singletonList(Literal.of(1L)), null, null),
            null
        );

    @Test
    public void testWindowFunctionIgnoreNullsFlagSerialisationFromV_4_7_0ToV_4_7_0() throws IOException {
        var output = new BytesStreamOutput();
        output.setVersion(Version.V_4_7_0);
        windowFunctionWithIgnoreNullsSetToTrue.writeTo(output);

        var in = output.bytes().streamInput();
        in.setVersion(Version.V_4_7_0);
        var actualWindowFunction = new WindowFunction(in);

        assertThat(actualWindowFunction).isEqualTo(windowFunctionWithIgnoreNullsSetToTrue);
    }

    @Test
    public void testWindowFunctionIgnoreNullsFlagSerialisationFromV_4_6_0ToV_4_7_0() throws IOException {
        var output = new BytesStreamOutput();
        output.setVersion(Version.V_4_6_0);
        windowFunctionWithIgnoreNullsSetToTrue.writeTo(output);

        var in = output.bytes().streamInput();
        in.setVersion(Version.V_4_6_0);
        var actualWindowFunction = new WindowFunction(in);

        assertThat(actualWindowFunction).isEqualTo(windowFunctionWithIgnoreNullsSetToNull);
    }
}
