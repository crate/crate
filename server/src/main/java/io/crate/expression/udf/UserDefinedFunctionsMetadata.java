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

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

public class UserDefinedFunctionsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "user_defined_functions";

    private final List<UserDefinedFunctionMetadata> functionsMetadata;

    private UserDefinedFunctionsMetadata(List<UserDefinedFunctionMetadata> functions) {
        this.functionsMetadata = functions;
    }

    public static UserDefinedFunctionsMetadata newInstance(UserDefinedFunctionsMetadata instance) {
        return new UserDefinedFunctionsMetadata(new ArrayList<>(instance.functionsMetadata));
    }

    @VisibleForTesting
    public static UserDefinedFunctionsMetadata of(UserDefinedFunctionMetadata... functions) {
        return new UserDefinedFunctionsMetadata(Arrays.asList(functions));
    }

    public void add(UserDefinedFunctionMetadata function) {
        functionsMetadata.add(function);
    }

    public void replace(UserDefinedFunctionMetadata function) {
        for (int i = 0; i < functionsMetadata.size(); i++) {
            if (functionsMetadata.get(i).sameSignature(function.schema(), function.name(), function.argumentTypes())) {
                functionsMetadata.set(i, function);
            }
        }
    }

    public boolean contains(String schema, String name, List<DataType<?>> types) {
        for (UserDefinedFunctionMetadata function : functionsMetadata) {
            if (function.sameSignature(schema, name, types)) {
                return true;
            }
        }
        return false;
    }

    public void remove(String schema, String name, List<DataType<?>> types) {
        for (ListIterator<UserDefinedFunctionMetadata> iter = functionsMetadata.listIterator(); iter.hasNext(); ) {
            if (iter.next().sameSignature(schema, name, types)) {
                iter.remove();
            }
        }
    }

    public List<UserDefinedFunctionMetadata> functionsMetadata() {
        return functionsMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(functionsMetadata.size());
        for (UserDefinedFunctionMetadata function : functionsMetadata) {
            function.writeTo(out);
        }
    }

    public UserDefinedFunctionsMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<UserDefinedFunctionMetadata> functions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            functions.add(new UserDefinedFunctionMetadata(in));
        }
        this.functionsMetadata = functions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("functions");
        for (UserDefinedFunctionMetadata function : functionsMetadata) {
            function.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static UserDefinedFunctionsMetadata fromXContent(XContentParser parser) throws IOException {
        List<UserDefinedFunctionMetadata> functions = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME && Objects.equals(parser.currentName(), "functions")) {
                if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        functions.add(UserDefinedFunctionMetadata.fromXContent(parser));
                    }
                }
            }
        }
        return new UserDefinedFunctionsMetadata(functions);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserDefinedFunctionsMetadata that = (UserDefinedFunctionsMetadata) o;
        return functionsMetadata.equals(that.functionsMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionsMetadata);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_1;
    }
}
