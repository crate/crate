/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.analyze;

import com.google.common.base.MoreObjects;
import io.crate.exceptions.UnhandledServerException;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.sql.tree.FunctionArgument;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FunctionArgumentDefinition implements Streamable, ToXContent {

    private String name;
    private DataType type;

    private FunctionArgumentDefinition(@Nullable String name, DataType dataType) {
        this.name = name;
        this.type = dataType;
    }

    private FunctionArgumentDefinition() {
    }

    public static FunctionArgumentDefinition of(String name, DataType dataType) {
        return new FunctionArgumentDefinition(name, dataType);
    }

    public static FunctionArgumentDefinition of(DataType dataType) {
        return new FunctionArgumentDefinition(null, dataType);
    }

    public static FunctionArgumentDefinition fromStream(StreamInput in) throws IOException {
        FunctionArgumentDefinition argumentDefinition = new FunctionArgumentDefinition();
        argumentDefinition.readFrom(in);
        return argumentDefinition;
    }

    public static List<FunctionArgumentDefinition> toFunctionArgumentDefinitions(List<FunctionArgument> arguments) {
        return arguments.stream()
            .map(arg -> FunctionArgumentDefinition.of(
                arg.name().orElse(null),
                DataTypeAnalyzer.convert(arg.type())))
            .collect(Collectors.toList());
    }

    public DataType type() {
        return type;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readOptionalString();
        type = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        DataTypes.toStream(type, out);
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
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("type", type).toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("name", name).field("data_type");
        UserDefinedFunctionMetaData.DataTypeXContent.toXContent(type, builder, params);
        builder.endObject();
        return builder;
    }

    public static FunctionArgumentDefinition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME || !"name".equals(parser.currentName())) {
            throw new IllegalStateException("Can't parse FunctionArgument from XContent, expected name field");
        }
        String name = parseStringField(parser);
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME || !"data_type".equals(parser.currentName())) {
            throw new IllegalStateException("Can't parse FunctionArgument from XContent, expected data_type field");
        }
        DataType type = UserDefinedFunctionMetaData.DataTypeXContent.fromXContent(parser);
        parser.nextToken();
        return new FunctionArgumentDefinition(name, type);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.VALUE_STRING && parser.currentToken() != XContentParser.Token.VALUE_NULL) {
            throw new UnhandledServerException("failed to parse function");
        }
        return parser.textOrNull();
    }
}
