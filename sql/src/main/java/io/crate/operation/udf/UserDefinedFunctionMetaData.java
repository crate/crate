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

package io.crate.operation.udf;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UnhandledServerException;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

public class UserDefinedFunctionMetaData implements Streamable, ToXContent {

    String name;
    List<FunctionArgumentDefinition> arguments;
    Set<String> options;
    DataType returnType;
    String functionLanguage;
    String functionBody;

    public UserDefinedFunctionMetaData(String name,
                                       List<FunctionArgumentDefinition> arguments,
                                       Set<String> options,
                                       DataType returnType,
                                       String functionLanguage,
                                       String functionBody) {
        this.name = name;
        this.arguments = arguments;
        this.options = options;
        this.returnType = returnType;
        this.functionLanguage = functionLanguage;
        this.functionBody = functionBody;
    }

    private UserDefinedFunctionMetaData() {
    }

    public static UserDefinedFunctionMetaData fromStream(StreamInput in) throws IOException {
        UserDefinedFunctionMetaData udfMetaData = new UserDefinedFunctionMetaData();
        udfMetaData.readFrom(in);
        return udfMetaData;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int numArguments = in.readVInt();
        arguments = new ArrayList<>(numArguments);
        for (int i = 0; i < numArguments; i++) {
            arguments.add(FunctionArgumentDefinition.fromStream(in));
        }
        int numOptions = in.readVInt();
        options = new HashSet<>(numOptions);
        for (int i = 0; i < numOptions; i++) {
            options.add(in.readString());
        }
        returnType = DataTypes.fromStream(in);
        functionLanguage = in.readString();
        functionBody = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(arguments.size());
        for (FunctionArgumentDefinition argument : arguments) {
            argument.writeTo(out);
        }
        out.writeVInt(options.size());
        for (String option : options) {
            out.writeString(option);
        }
        DataTypes.toStream(returnType, out);
        out.writeString(functionLanguage);
        out.writeString(functionBody);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.startArray("arguments");
        for(FunctionArgumentDefinition argument : arguments) {
            argument.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray("options");
        for (String option : options) {
            builder.value(option);
        }
        builder.endArray();
        builder.field("return_type");
        DataTypes.toXContent(returnType, builder, params);
        builder.field("language", functionLanguage);
        builder.field("body", functionBody);
        builder.endObject();
        return builder;
    }

    public static UserDefinedFunctionMetaData fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String name = null;
        List<FunctionArgumentDefinition> arguments = new ArrayList<>();
        Set<String> options = new HashSet<>();
        DataType returnType = null;
        String language = null;
        String body = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                if ("name".equals(parser.currentName())) {
                    name = parseStringField(parser);
                } else if ("language".equals(parser.currentName())) {
                    language = parseStringField(parser);
                } else if ("body".equals(parser.currentName())) {
                    body = parseStringField(parser);
                } else if ("arguments".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                        throw new UnhandledServerException(
                            String.format(Locale.ENGLISH, "failed to parse function")
                        );
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        arguments.add(FunctionArgumentDefinition.fromXContent(parser));
                    }
                } else if ("options".equals(parser.currentName())) {
                    if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                        throw new UnhandledServerException(
                            String.format(Locale.ENGLISH, "failed to parse function")
                        );
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        options.add(parser.text());
                    }
                } else if ("return_type".equals(parser.currentName())){
                    returnType = DataTypes.fromXContent(parser);
                } else {
                    throw new UnhandledServerException(
                        String.format(Locale.ENGLISH, "failed to parse function")
                    );
                }
            } else {
                throw new UnhandledServerException(
                    String.format(Locale.ENGLISH, "failed to parse function")
                );
            }
        }
        return new UserDefinedFunctionMetaData(name, arguments, options, returnType, language, body);
    }

    private static String parseStringField(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.VALUE_STRING && parser.currentToken() != XContentParser.Token.VALUE_NULL) {
            throw new UnhandledServerException(
                String.format(Locale.ENGLISH, "failed to parse function")
            );
        }
        return parser.textOrNull();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserDefinedFunctionMetaData that = (UserDefinedFunctionMetaData) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(arguments, that.arguments) &&
            Objects.equals(options, that.options) &&
            Objects.equals(returnType, that.returnType) &&
            Objects.equals(functionLanguage, that.functionLanguage) &&
            Objects.equals(functionBody, that.functionBody);

    }

    @Override
    public int hashCode() {
        return Objects.hash(name, arguments, options, returnType, functionBody, functionLanguage);
    }
}
