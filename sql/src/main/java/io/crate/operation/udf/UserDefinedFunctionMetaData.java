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

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UnhandledServerException;
import io.crate.types.*;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class UserDefinedFunctionMetaData implements Streamable, ToXContent {

    private String name;
    private String schema;
    private List<FunctionArgumentDefinition> arguments;
    DataType returnType;
    private List<DataType> argumentTypes;
    String language;
    String definition;

    public UserDefinedFunctionMetaData(String schema,
                                       String name,
                                       List<FunctionArgumentDefinition> arguments,
                                       DataType returnType,
                                       String language,
                                       String definition) {
        this.schema = schema;
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
        this.language = language;
        this.definition = definition;
        this.argumentTypes = argumentTypesFrom(arguments);
    }

    private UserDefinedFunctionMetaData() {
    }

    public static UserDefinedFunctionMetaData fromStream(StreamInput in) throws IOException {
        UserDefinedFunctionMetaData udfMetaData = new UserDefinedFunctionMetaData();
        udfMetaData.readFrom(in);
        return udfMetaData;
    }

    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public DataType returnType() {
        return returnType;
    }

    public String language() {
        return language;
    }

    public String definition() {
        return definition;
    }

    public List<FunctionArgumentDefinition> arguments() {
        return arguments;
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    boolean sameSignature(String schema, String name, List<DataType> types) {
        return this.schema().equals(schema) && this.name().equals(name) && this.argumentTypes().equals(types);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        schema = in.readString();
        name = in.readString();
        int numArguments = in.readVInt();
        arguments = new ArrayList<>(numArguments);
        for (int i = 0; i < numArguments; i++) {
            arguments.add(FunctionArgumentDefinition.fromStream(in));
        }
        argumentTypes = argumentTypesFrom(arguments());
        returnType = DataTypes.fromStream(in);
        language = in.readString();
        definition = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(schema);
        out.writeString(name);
        out.writeVInt(arguments.size());
        for (FunctionArgumentDefinition argument : arguments) {
            argument.writeTo(out);
        }
        DataTypes.toStream(returnType, out);
        out.writeString(language);
        out.writeString(definition);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("schema", schema);
        builder.field("name", name);
        builder.startArray("arguments");
        for (FunctionArgumentDefinition argument : arguments) {
            argument.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("return_type");
        DataTypeXContent.toXContent(returnType, builder, params);
        builder.field("language", language);
        builder.field("definition", definition);
        builder.endObject();
        return builder;
    }

    static UserDefinedFunctionMetaData fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String schema = null;
        String name = null;
        List<FunctionArgumentDefinition> arguments = new ArrayList<>();
        DataType returnType = null;
        String language = null;
        String definition = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                if ("schema".equals(parser.currentName())) {
                    schema = parseStringField(parser);
                } else if ("name".equals(parser.currentName())) {
                    name = parseStringField(parser);
                } else if ("language".equals(parser.currentName())) {
                    language = parseStringField(parser);
                } else if ("definition".equals(parser.currentName())) {
                    definition = parseStringField(parser);
                } else if ("arguments".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                        throw new UnhandledServerException("failed to parse function");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        arguments.add(FunctionArgumentDefinition.fromXContent(parser));
                    }
                } else if ("return_type".equals(parser.currentName())) {
                    returnType = DataTypeXContent.fromXContent(parser);
                } else {
                    throw new UnhandledServerException("failed to parse function");
                }
            } else {
                throw new UnhandledServerException("failed to parse function");
            }
        }
        return new UserDefinedFunctionMetaData(schema, name, arguments, returnType, language, definition);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.VALUE_STRING && parser.currentToken()
            != XContentParser.Token.VALUE_NULL) {
            throw new UnhandledServerException("failed to parse function");
        }
        return parser.textOrNull();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserDefinedFunctionMetaData that = (UserDefinedFunctionMetaData) o;
        return Objects.equals(schema, that.schema) &&
            Objects.equals(name, that.name) &&
            Objects.equals(arguments, that.arguments) &&
            Objects.equals(returnType, that.returnType) &&
            Objects.equals(language, that.language) &&
            Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, arguments, returnType, definition, language);
    }

    public static class DataTypeXContent {

        public static XContentBuilder toXContent(DataType type, XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("id", type.id());
            if (type instanceof CollectionType) {
                builder.field("inner_type");
                toXContent(((CollectionType) type).innerType(), builder, params);
            }
            builder.endObject();
            return builder;
        }

        public static DataType fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            DataType type = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    int id = parser.intValue();
                    if (id == ArrayType.ID || id == SetType.ID) {
                        if (parser.nextToken() != XContentParser.Token.FIELD_NAME || !"inner_type".equals(parser.currentName())) {
                            throw new IllegalStateException("Can't parse DataType form XContent");
                        }
                        DataType innerType = fromXContent(parser);
                        if (id == ArrayType.ID) {
                            type = new ArrayType(innerType);
                        } else {
                            type = new SetType(innerType);
                        }
                    } else {
                        type = DataTypes.fromId(id);
                    }
                }
            }
            return type;
        }
    }

    static List<DataType> argumentTypesFrom(List<FunctionArgumentDefinition> arguments) {
        return arguments.stream().map(FunctionArgumentDefinition::type).collect(toList());
    }

}
