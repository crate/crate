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
package io.crate.operation.udf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;

public class UserDefinedFunctionsMetaDataTest extends CrateUnitTest {

    @Test
    public void testUserDefinedFunctionStreaming() throws IOException {
        String functionBody = "function(a, b) {return a - b;}";
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            "my_add",
            ImmutableList.of(FunctionArgumentDefinition.of(DataTypes.DOUBLE_ARRAY),
                FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
            ),
            ImmutableSet.of("STRICT"),
            DataTypes.FLOAT,
            "javascript",
            functionBody
        );

        BytesStreamOutput out = new BytesStreamOutput();
        udfMeta.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        UserDefinedFunctionMetaData udfMeta2 = UserDefinedFunctionMetaData.fromStream(in);
        assertThat(udfMeta, is(udfMeta2));

        assertThat(udfMeta2.name, is("my_add"));
        assertThat(udfMeta2.arguments.size(), is(2));
        assertThat(udfMeta2.arguments.get(1), is(
            FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
        ));
        assertThat(udfMeta2.options, is(ImmutableSet.of("STRICT")));
        assertThat(udfMeta2.returnType, is(DataTypes.FLOAT));
        assertThat(udfMeta2.functionLanguage, is("javascript"));
        assertThat(udfMeta2.functionBody, is(functionBody));
    }
}
