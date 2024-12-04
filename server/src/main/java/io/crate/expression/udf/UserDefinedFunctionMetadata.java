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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UnhandledServerException;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class UserDefinedFunctionMetadata implements Writeable {

    private final String name;
    private final String schema;
    private final List<FunctionArgumentDefinition> arguments;
    private final DataType<?> returnType;
    private final List<DataType<?>> argumentTypes;
    private final String language;
    private final String definition;
    private final String specificName;

    public UserDefinedFunctionMetadata(String schema,
                                       String name,
                                       List<FunctionArgumentDefinition> arguments,
                                       DataType<?> returnType,
                                       String language,
                                       String definition) {
        this.schema = schema;
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
        this.language = language;
        this.definition = definition;
        this.argumentTypes = argumentTypesFrom(arguments);
        this.specificName = specificName(name, argumentTypes);
    }

    public UserDefinedFunctionMetadata(StreamInput in) throws IOException {
        schema = in.readString();
        name = in.readString();
        int numArguments = in.readVInt();
        arguments = new ArrayList<>(numArguments);
        for (int i = 0; i < numArguments; i++) {
            arguments.add(new FunctionArgumentDefinition(in));
        }
        argumentTypes = argumentTypesFrom(arguments());
        returnType = DataTypes.fromStream(in);
        language = in.readString();
        definition = in.readString();
        specificName = specificName(name, argumentTypes);
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

    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public DataType<?> returnType() {
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

    public List<DataType<?>> argumentTypes() {
        return argumentTypes;
    }

    public String specificName() {
        return specificName;
    }

    boolean sameSignature(String schema, String name, List<DataType<?>> types) {
        return this.schema().equals(schema) && this.name().equals(name) && this.argumentTypes().equals(types);
    }

    static UserDefinedFunctionMetadata fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
        }
        XContentParser.Token token;
        String schema = null;
        String name = null;
        List<FunctionArgumentDefinition> arguments = new ArrayList<>();
        DataType<?> returnType = null;
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
                        throw new IllegalArgumentException("Expected a START_ARRAY but got " + parser.currentToken());
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        arguments.add(FunctionArgumentDefinition.fromXContent(parser));
                    }
                } else if ("return_type".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
                    }
                    returnType = DataTypeXContent.fromXContent(parser);
                } else {
                    throw new IllegalArgumentException("Got unexpected FIELD_NAME " + parser.currentToken());
                }
            } else {
                throw new IllegalArgumentException("Expected a FIELD_NAME but got " + parser.currentToken());
            }
        }
        return new UserDefinedFunctionMetadata(schema, name, arguments, returnType, language, definition);
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

        UserDefinedFunctionMetadata that = (UserDefinedFunctionMetadata) o;
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

        public static DataType<?> fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
            }
            int id = DataTypes.NOT_SUPPORTED.id();
            DataType<?> innerType = DataTypes.UNDEFINED;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("id".equals(fieldName)) {
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new IllegalArgumentException("Expected a VALUE_NUMBER but got " + parser.currentToken());
                        }
                        id = parser.intValue();
                    } else if ("inner_type".equals(fieldName)) {
                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                            throw new IllegalArgumentException("Expected a START_OBJECT but got " + parser.currentToken());
                        }
                        innerType = fromXContent(parser);
                    }
                }
            }
            if (id == ArrayType.ID) {
                return new ArrayType<>(innerType);
            }
            return DataTypes.fromId(id);
        }
    }

    static List<DataType<?>> argumentTypesFrom(List<FunctionArgumentDefinition> arguments) {
        return arguments.stream().map(FunctionArgumentDefinition::type).collect(toList());
    }

    static String specificName(String name, List<DataType<?>> types) {
        return String.format(Locale.ENGLISH, "%s(%s)", name,
            types.stream().map(DataType::getName).collect(Collectors.joining(", ")));
    }

}
