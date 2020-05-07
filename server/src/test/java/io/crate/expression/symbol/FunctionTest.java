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

package io.crate.expression.symbol;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FunctionTest extends CrateUnitTest {

    private FunctionIdent functionIdent = new FunctionIdent(
        randomAsciiLettersOfLength(10),
        List.of(DataTypes.BOOLEAN)
    );

    @Test
    public void test_serialization_without_filter() throws Exception {
        Function fn = new Function(
            new FunctionInfo(
                functionIdent,
                TestingHelpers.randomPrimitiveType(),
                FunctionInfo.Type.SCALAR,
                randomFeatures()
            ),
            List.of(createReference(randomAsciiLettersOfLength(2), DataTypes.BOOLEAN))
        );

        BytesStreamOutput output = new BytesStreamOutput();
        Symbols.toStream(fn, output);

        StreamInput input = output.bytes().streamInput();
        Function fn2 = (Function) Symbols.fromStream(input);

        assertThat(fn, is(fn2));
    }

    @Test
    public void test_serialization_with_filter() throws Exception {
        Function fn = new Function(
            new FunctionInfo(
                functionIdent,
                TestingHelpers.randomPrimitiveType(),
                FunctionInfo.Type.SCALAR,
                randomFeatures()
            ),
            null,
            List.of(createReference(randomAsciiLettersOfLength(2), DataTypes.BOOLEAN)),
            Literal.of(true)
        );

        BytesStreamOutput output = new BytesStreamOutput();
        Symbols.toStream(fn, output);

        StreamInput input = output.bytes().streamInput();
        Function fn2 = (Function) Symbols.fromStream(input);

        assertThat(fn2.filter(), not(nullValue()));
        assertThat(fn, is(fn2));
    }

    @Test
    public void test_serialization_before_version_4_1_0() throws Exception {
        Function fn = new Function(
            new FunctionInfo(
                functionIdent,
                TestingHelpers.randomPrimitiveType(),
                FunctionInfo.Type.SCALAR,
                randomFeatures()
            ),
            List.of(createReference(randomAsciiLettersOfLength(2), DataTypes.BOOLEAN))
        );

        var output = new BytesStreamOutput();
        output.setVersion(Version.V_4_0_0);
        Symbols.toStream(fn, output);

        var input = output.bytes().streamInput();
        input.setVersion(Version.V_4_0_0);
        Function fn2 = (Function) Symbols.fromStream(input);

        assertThat(fn2.filter(), is(nullValue()));
        assertThat(fn, is(fn2));
    }

    private static Set<FunctionInfo.Feature> randomFeatures() {
        Set<FunctionInfo.Feature> features = EnumSet.noneOf(FunctionInfo.Feature.class);
        for (FunctionInfo.Feature feature : FunctionInfo.Feature.values()) {
            if (randomBoolean()) {
                features.add(feature);
            }
        }
        return features;
    }
}
