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

import io.crate.types.DataType;
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
            if (functionsMetaData.get(i).sameSignature(function.schema(), function.name(), function.argumentTypes())) {
                functionsMetaData.set(i, function);
            }
        }
    }

    public boolean contains(String schema, String name, List<DataType> types) {
        for (UserDefinedFunctionMetaData function : functionsMetaData) {
            if (function.sameSignature(schema, name, types)) {
                return true;
            }
        }
        return false;
    }

    public void remove(String schema, String name, List<DataType> types) {
        for (ListIterator<UserDefinedFunctionMetaData> iter = functionsMetaData.listIterator(); iter.hasNext(); ) {
            if (iter.next().sameSignature(schema, name, types)) {
                iter.remove();
            }
        }
    }

    public List<UserDefinedFunctionMetaData> functionsMetaData() {
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
