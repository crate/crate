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

package io.crate.analyze.symbol;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FunctionTest extends CrateUnitTest {

    @Test
    public void testSerialization() throws Exception {
        Function fn = new Function(
                new FunctionInfo(
                        new FunctionIdent(
                                randomAsciiOfLength(10),
                                ImmutableList.<DataType>of(DataTypes.BOOLEAN)
                        ),
                        TestingHelpers.randomPrimitiveType(), FunctionInfo.Type.SCALAR, randomBoolean(), randomBoolean()),
                Arrays.<Symbol>asList(TestingHelpers.createReference(randomAsciiOfLength(2), DataTypes.BOOLEAN))
        );

        BytesStreamOutput output = new BytesStreamOutput();
        Symbol.toStream(fn, output);

        StreamInput input = StreamInput.wrap(output.bytes());
        Function fn2 = (Function)Symbol.fromStream(input);

        assertThat(fn, is(equalTo(fn2)));
        assertThat(fn.hashCode(), is(fn2.hashCode()));
    }

    @Test
    public void testCloning() throws Exception {

        Function fn = new Function(
                new FunctionInfo(
                        new FunctionIdent(
                                randomAsciiOfLength(10),
                                ImmutableList.<DataType>of(DataTypes.BOOLEAN)
                        ),
                        TestingHelpers.randomPrimitiveType(), FunctionInfo.Type.SCALAR, randomBoolean(), randomBoolean()),
                Arrays.<Symbol>asList(TestingHelpers.createReference(randomAsciiOfLength(2), DataTypes.BOOLEAN))
        );

        Function fn2 = fn.clone();

        assertThat(fn, is(equalTo(fn2)));
        assertThat(fn.hashCode(), is(fn2.hashCode()));

    }
}
