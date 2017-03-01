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

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;

public class UserDefinedFunctionMetaData implements Streamable {

    String name;
    List<FunctionArgument> arguments;
    Set<String> options;
    DataType returnType;
    String functionLanguage;
    String functionBody;

    public UserDefinedFunctionMetaData(String name, List<FunctionArgument> arguments, Set<String> options,
                                       DataType returnType, String functionLanguage, String functionBody) {
        this.name = name;
        this.arguments = arguments;
        this.options = options;
        this.returnType = returnType;
        this.functionLanguage = functionLanguage;
        this.functionBody = functionBody;
    }

    private UserDefinedFunctionMetaData() {}

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
            FunctionArgument argument = new FunctionArgument();
            argument.readFrom(in);
            arguments.add(argument);
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
        for (FunctionArgument argument : arguments) {
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

    public static class FunctionArgument implements Streamable {

        private String name;
        private DataType dataType;

        public FunctionArgument(DataType dataType, @Nullable String name) {
            this.name = name;
            this.dataType = dataType;
        }

        private FunctionArgument() {}

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readOptionalString();
            dataType = DataTypes.fromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(name);
            DataTypes.toStream(dataType, out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FunctionArgument argument = (FunctionArgument) o;
            return Objects.equals(name, argument.name) &&
                   Objects.equals(dataType, argument.dataType);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + dataType.hashCode();
            return result;
        }
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
        int result = name.hashCode();
        result = 31 * result + arguments.hashCode();
        result = 31 * result + options.hashCode();
        result = 31 * result + returnType.hashCode();
        result = 31 * result + functionLanguage.hashCode();
        result = 31 * result + functionBody.hashCode();
        return result;
    }
}
