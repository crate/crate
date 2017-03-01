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

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

public class UserDefinedFunctionsMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "user_defined_functions";

    static final UserDefinedFunctionsMetaData PROTO = new UserDefinedFunctionsMetaData();

    static {
        // register non plugin custom metadata
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final List<UserDefinedFunctionMetaData> functionsMetaData;

    private UserDefinedFunctionsMetaData() {
        this.functionsMetaData = new ArrayList<>();
    }

    private UserDefinedFunctionsMetaData(List<UserDefinedFunctionMetaData> functions) {
        this.functionsMetaData = functions;
    }

    public static UserDefinedFunctionsMetaData newInstance(UserDefinedFunctionsMetaData instance) {
        return new UserDefinedFunctionsMetaData(new ArrayList<>(instance.functionsMetaData));
    }

    static UserDefinedFunctionsMetaData of(UserDefinedFunctionMetaData... functions) {
        return new UserDefinedFunctionsMetaData(Arrays.asList(functions));
    }

    public void add(UserDefinedFunctionMetaData function) {
        functionsMetaData.add(function);
    }

    public void replace(UserDefinedFunctionMetaData function) {
        for (int i = 0; i < functionsMetaData.size(); i++) {
            if (functionsMetaData.get(i).sameSignature(function)) {
                functionsMetaData.set(i, function);
            }
        }
    }

    public boolean contains(UserDefinedFunctionMetaData functionMetaData) {
        for (UserDefinedFunctionMetaData function : functionsMetaData) {
            if (functionMetaData.sameSignature(function)) {
                return true;
            }
        }
        return false;
    }

    public Collection<UserDefinedFunctionMetaData> functionsMetaData() {
        return functionsMetaData;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(functionsMetaData.size());
        for (UserDefinedFunctionMetaData function : functionsMetaData) {
            function.writeTo(out);
        }
    }

    @Override
    public MetaData.Custom readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<UserDefinedFunctionMetaData> functions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            functions.add(UserDefinedFunctionMetaData.fromStream(in));
        }
        return new UserDefinedFunctionsMetaData(functions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("functions");
        for (UserDefinedFunctionMetaData function : functionsMetaData) {
            function.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
        List<UserDefinedFunctionMetaData> functions = new ArrayList<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME && Objects.equals(parser.currentName(), "functions")) {
            if ((parser.nextToken()) == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
                    functions.add(UserDefinedFunctionMetaData.fromXContent(parser));
                }
            }
        }
        return new UserDefinedFunctionsMetaData(functions);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserDefinedFunctionsMetaData that = (UserDefinedFunctionsMetaData) o;
        return functionsMetaData.equals(that.functionsMetaData);
    }

}
