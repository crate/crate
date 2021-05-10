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

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.expression.udf.UserDefinedFunctionMetadata.argumentTypesFrom;
import static io.crate.expression.udf.UserDefinedFunctionMetadata.specificName;
import static org.hamcrest.core.Is.is;

public class UserDefinedFunctionsMetadataTest extends ESTestCase {

    private static String definition = "function(a, b) {return a - b;}";
    private static List<FunctionArgumentDefinition> args = List.of(
        FunctionArgumentDefinition.of(DataTypes.DOUBLE_ARRAY),
        FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
    );
    private static final UserDefinedFunctionMetadata FUNCTION_METADATA = new UserDefinedFunctionMetadata(
        "my_schema",
        "my_add",
        args,
        DataTypes.FLOAT,
        "dummy_lang",
       definition
    );

    public static final UserDefinedFunctionsMetadata DUMMY_UDF_METADATA = UserDefinedFunctionsMetadata.of(FUNCTION_METADATA);

    @Test
    public void testUserDefinedFunctionStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        FUNCTION_METADATA.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UserDefinedFunctionMetadata udfMeta2 = new UserDefinedFunctionMetadata(in);
        assertThat(FUNCTION_METADATA, is(udfMeta2));

        assertThat(udfMeta2.schema(), is("my_schema"));
        assertThat(udfMeta2.name(), is("my_add"));
        assertThat(udfMeta2.arguments().size(), is(2));
        assertThat(udfMeta2.arguments().get(1), is(
            FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
        ));
        assertThat(udfMeta2.argumentTypes().size(), is(2));
        assertThat(udfMeta2.argumentTypes().get(1), is(DataTypes.DOUBLE));
        assertThat(udfMeta2.returnType(), is(DataTypes.FLOAT));
        assertThat(udfMeta2.language(), is("dummy_lang"));
        assertThat(udfMeta2.definition(), is(definition));
    }

    @Test
    public void testUserDefinedFunctionToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        DUMMY_UDF_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // start object
        UserDefinedFunctionsMetadata functions = UserDefinedFunctionsMetadata.fromXContent(parser);
        assertEquals(DUMMY_UDF_METADATA, functions);
    }

    @Test
    public void testUserDefinedFunctionToXContentWithEmptyMetadata() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        UserDefinedFunctionsMetadata functions = UserDefinedFunctionsMetadata.of();
        functions.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken(); // enter START_OBJECT
        UserDefinedFunctionsMetadata functions2 = UserDefinedFunctionsMetadata.fromXContent(parser);
        assertEquals(functions, functions2);
    }

    @Test
    public void testDataTypeStreaming() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        var type = new ArrayType<>(new ArrayType<>(DataTypes.STRING));
        UserDefinedFunctionMetadata.DataTypeXContent.toXContent(type, builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(BytesReference.bytes(builder)));
        parser.nextToken();  // enter START_OBJECT
        ArrayType type2 = (ArrayType) UserDefinedFunctionMetadata.DataTypeXContent.fromXContent(parser);
        assertTrue(type.equals(type2));
    }

    @Test
    public void testSameSignature() throws Exception {
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "my_add", argumentTypesFrom(args)), is(true));
        assertThat(FUNCTION_METADATA.sameSignature("different_schema", "my_add", argumentTypesFrom(args)), is(false));
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "different_name", argumentTypesFrom(args)), is(false));
        assertThat(FUNCTION_METADATA.sameSignature("my_schema", "my_add", List.of()), is(false));
    }

    @Test
    public void testSpecificName() throws Exception {
        assertThat(specificName("my_func", List.of()), is("my_func()"));
        assertThat(specificName("my_func", List.of(DataTypes.BOOLEAN, new ArrayType<>(DataTypes.BOOLEAN))),
            is("my_func(boolean, boolean_array)"));
    }
}
