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

package io.crate.expression.udf;

import static io.crate.expression.udf.UserDefinedFunctionMetadata.argumentTypesFrom;
import static io.crate.expression.udf.UserDefinedFunctionMetadata.specificName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class UserDefinedFunctionsMetadataTest extends ESTestCase {

    private static final String DEFINITION = "function(a, b) {return a - b;}";
    private static final List<FunctionArgumentDefinition> ARGS = List.of(
        FunctionArgumentDefinition.of(DataTypes.DOUBLE_ARRAY),
        FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
    );
    private static final UserDefinedFunctionMetadata FUNCTION_METADATA = new UserDefinedFunctionMetadata(
        "my_schema",
        "my_add",
        ARGS,
        DataTypes.FLOAT,
        "dummy_lang",
        DEFINITION
    );

    public static final UserDefinedFunctionsMetadata DUMMY_UDF_METADATA = UserDefinedFunctionsMetadata.of(FUNCTION_METADATA);

    @Test
    public void testUserDefinedFunctionStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        FUNCTION_METADATA.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UserDefinedFunctionMetadata udfMeta2 = new UserDefinedFunctionMetadata(in);
        assertThat(udfMeta2).isEqualTo(FUNCTION_METADATA);

        assertThat(udfMeta2.schema()).isEqualTo("my_schema");
        assertThat(udfMeta2.name()).isEqualTo("my_add");
        assertThat(udfMeta2.arguments()).hasSize(2);
        assertThat(udfMeta2.arguments().get(1)).isEqualTo(
            FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
        );
        assertThat(udfMeta2.argumentTypes()).hasSize(2);
        assertThat(udfMeta2.argumentTypes().get(1)).isEqualTo(DataTypes.DOUBLE);
        assertThat(udfMeta2.returnType()).isEqualTo(DataTypes.FLOAT);
        assertThat(udfMeta2.language()).isEqualTo("dummy_lang");
        assertThat(udfMeta2.definition()).isEqualTo(DEFINITION);
    }

    @Test
    public void testSameSignature() throws Exception {
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "my_add", argumentTypesFrom(ARGS))).isTrue();
        assertThat(FUNCTION_METADATA.sameSignature("different_schema", "my_add", argumentTypesFrom(ARGS))).isFalse();
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "different_name", argumentTypesFrom(ARGS))).isFalse();
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "my_add", List.of())).isFalse();
    }

    @Test
    public void testSpecificName() throws Exception {
        assertThat(specificName("my_func", List.of())).isEqualTo("my_func()");
        assertThat(specificName("my_func", List.of(DataTypes.BOOLEAN, new ArrayType<>(DataTypes.BOOLEAN)))).isEqualTo("my_func(boolean, boolean_array)");
    }
}
