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

    public static final UserDefinedFunctionsMetaData PROTO = new UserDefinedFunctionsMetaData();

    static {
        // register non plugin custom metadata
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final Map<Integer, UserDefinedFunctionMetaData> functionsBySignatureHash;

    private UserDefinedFunctionsMetaData() {
        this.functionsBySignatureHash = new HashMap<>();
    }

    private UserDefinedFunctionsMetaData(Map<Integer, UserDefinedFunctionMetaData> functions) {
        this.functionsBySignatureHash = functions;
    }

    public static UserDefinedFunctionsMetaData newInstance(UserDefinedFunctionsMetaData instance) {
        return new UserDefinedFunctionsMetaData(new HashMap<>(instance.functionsBySignatureHash));
    }

    static UserDefinedFunctionsMetaData of(UserDefinedFunctionMetaData... functions) {
        Map<Integer, UserDefinedFunctionMetaData> udfs = new HashMap<>();
        for (UserDefinedFunctionMetaData udf : functions) {
            udfs.put(udf.createMethodSignature(), udf);
        }
        return new UserDefinedFunctionsMetaData(udfs);
    }

    public void put(UserDefinedFunctionMetaData function) {
        functionsBySignatureHash.put(function.createMethodSignature(), function);
    }

    public boolean contains(UserDefinedFunctionMetaData function) {
        return functionsBySignatureHash.containsKey(function.createMethodSignature());
    }

    public Collection<UserDefinedFunctionMetaData> functionsMetaData() {
        return functionsBySignatureHash.values();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(functionsBySignatureHash.size());
        for (UserDefinedFunctionMetaData function : functionsBySignatureHash.values()) {
            function.writeTo(out);
        }
    }

    @Override
    public MetaData.Custom readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<Integer, UserDefinedFunctionMetaData> functions = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            UserDefinedFunctionMetaData function = UserDefinedFunctionMetaData.fromStream(in);
            functions.put(function.createMethodSignature(), function);
        }
        return new UserDefinedFunctionsMetaData(functions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("functions");
        for (UserDefinedFunctionMetaData function : functionsBySignatureHash.values()) {
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
        Map<Integer, UserDefinedFunctionMetaData> functions = new HashMap<>();
        if ((parser.nextToken()) == XContentParser.Token.START_ARRAY) {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
                UserDefinedFunctionMetaData function = UserDefinedFunctionMetaData.fromXContent(parser);
                functions.put(function.createMethodSignature(), function);
            }
        }
        return new UserDefinedFunctionsMetaData(functions);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.API_AND_SNAPSHOT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserDefinedFunctionsMetaData that = (UserDefinedFunctionsMetaData) o;
        return functionsBySignatureHash.equals(that.functionsBySignatureHash);
    }
}
