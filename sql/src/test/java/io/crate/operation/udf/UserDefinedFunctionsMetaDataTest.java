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
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.operation.udf.UserDefinedFunctionMetaData.argumentTypesFrom;
import static org.hamcrest.core.Is.is;

public class UserDefinedFunctionsMetaDataTest extends CrateUnitTest {

    private String definition = "function(a, b) {return a - b;}";
    private List<FunctionArgumentDefinition> args = ImmutableList.of(
        FunctionArgumentDefinition.of(DataTypes.DOUBLE_ARRAY),
        FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
    );
    private UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
        "my_schema",
        "my_add",
        args,
        DataTypes.FLOAT,
        "javascript",
       definition
    );

    @Test
    public void testUserDefinedFunctionStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        udfMeta.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        UserDefinedFunctionMetaData udfMeta2 = UserDefinedFunctionMetaData.fromStream(in);
        assertThat(udfMeta, is(udfMeta2));

        assertThat(udfMeta2.schema(), is("my_schema"));
        assertThat(udfMeta2.name(), is("my_add"));
        assertThat(udfMeta2.arguments().size(), is(2));
        assertThat(udfMeta2.arguments().get(1), is(
            FunctionArgumentDefinition.of("my_named_arg", DataTypes.DOUBLE)
        ));
        assertThat(udfMeta2.argumentTypes().size(), is(2));
        assertThat(udfMeta2.argumentTypes().get(1), is(DataTypes.DOUBLE));
        assertThat(udfMeta2.returnType, is(DataTypes.FLOAT));
        assertThat(udfMeta2.language, is("javascript"));
        assertThat(udfMeta2.definition, is(definition));
    }

    @Test
    public void testUserDefinedFunctionToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        UserDefinedFunctionsMetaData functions = UserDefinedFunctionsMetaData.of(udfMeta);
        functions.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken(); // start object
        UserDefinedFunctionsMetaData functions2 =
            (UserDefinedFunctionsMetaData) UserDefinedFunctionsMetaData.of().fromXContent(parser);
        assertEquals(functions, functions2);
    }

    @Test
    public void testUserDefinedFunctionToXContentWithEmptyMetadata() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();
        UserDefinedFunctionsMetaData functions = UserDefinedFunctionsMetaData.of();
        functions.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        UserDefinedFunctionsMetaData functions2 =
            (UserDefinedFunctionsMetaData) UserDefinedFunctionsMetaData.of().fromXContent(parser);
        assertEquals(functions, functions2);
    }

    @Test
    public void testDataTypeStreaming() throws Exception {
        ArrayType type = new ArrayType(new ArrayType(StringType.INSTANCE));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        UserDefinedFunctionMetaData.DataTypeXContent.toXContent(type, builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        ArrayType type2 = (ArrayType) UserDefinedFunctionMetaData.DataTypeXContent.fromXContent(parser);
        assertTrue(type.equals(type2));
    }

    @Test
    public void testSameSignature() throws Exception {
        assertThat(udfMeta.sameSignature("my_schema", "my_add", argumentTypesFrom(args)), is(true));
        assertThat(udfMeta.sameSignature("different_schema", "my_add", argumentTypesFrom(args)), is(false));
        assertThat(udfMeta.sameSignature("my_schema", "different_name", argumentTypesFrom(args)), is(false));
        assertThat(udfMeta.sameSignature("my_schema", "my_add", ImmutableList.of()), is(false));
    }
}
