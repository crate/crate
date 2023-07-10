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

package io.crate.analyze;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class FunctionArgumentDefinition implements Writeable, ToXContent {

    @Nullable
    private final String name;
    private final DataType<?> type;

    private FunctionArgumentDefinition(@Nullable String name, DataType<?> dataType) {
        this.name = name;
        this.type = dataType;
    }

    public FunctionArgumentDefinition(StreamInput in) throws IOException {
        name = in.readOptionalString();
        type = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        DataTypes.toStream(type, out);
    }

    public static FunctionArgumentDefinition of(String name, DataType<?> dataType) {
        return new FunctionArgumentDefinition(name, dataType);
    }

    public static FunctionArgumentDefinition of(DataType<?> dataType) {
        return new FunctionArgumentDefinition(null, dataType);
    }

    public DataType<?> type() {
        return type;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final FunctionArgumentDefinition that = (FunctionArgumentDefinition) o;
        return Objects.equals(this.name, that.name)
            && Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "FunctionArgumentDefinition{" +
               "name='" + name + '\'' +
               ", type=" + type +
               '}';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("name", name).field("data_type");
        UserDefinedFunctionMetadata.DataTypeXContent.toXContent(type, builder, params);
        builder.endObject();
        return builder;
    }

    public static FunctionArgumentDefinition fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
        }
        String name = null;
        DataType type = DataTypes.UNDEFINED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                if ("name".equals(parser.currentName())) {
                    name = parseStringField(parser);
                } else if ("data_type".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
                    }
                    type = UserDefinedFunctionMetadata.DataTypeXContent.fromXContent(parser);
                } else {
                    throw new IllegalArgumentException("Expected \"name\" or \"data_type\", but got " + parser.currentName());
                }
            } else {
                throw new IllegalArgumentException("Expected a FIELD_NAME but got " + parser.currentToken());
            }
        }
        return new FunctionArgumentDefinition(name, type);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.VALUE_STRING && parser.currentToken() != XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("Failed to parse string field");
        }
        return parser.textOrNull();
    }
}
