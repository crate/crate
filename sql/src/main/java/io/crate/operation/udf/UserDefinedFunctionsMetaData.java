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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

public class UserDefinedFunctionsMetaData extends AbstractDiffable<MetaData.Custom> implements MetaData.Custom{

    public static final String TYPE = "user_defined_functions";

    public static final UserDefinedFunctionsMetaData PROTO = new UserDefinedFunctionsMetaData();

    static {
        // register non plugin custom metadata
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final List<UserDefinedFunctionMetaData> functions;

    public UserDefinedFunctionsMetaData(UserDefinedFunctionMetaData... functions) {
        this.functions = Arrays.asList(functions);
    }

    public List<UserDefinedFunctionMetaData> functions() {
        return functions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(functions.size());
        for (UserDefinedFunctionMetaData function : functions) {
            function.writeTo(out);
        }
    }

    @Override
    public MetaData.Custom readFrom(StreamInput in) throws IOException {
        UserDefinedFunctionMetaData[] functions = new UserDefinedFunctionMetaData[in.readVInt()];
        for (int i = 0; i < functions.length; i++) {
            functions[i] = UserDefinedFunctionMetaData.fromStream(in);
        }
        return new UserDefinedFunctionsMetaData(functions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (UserDefinedFunctionMetaData function : functions) {
            function.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public MetaData.Custom fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<UserDefinedFunctionMetaData> functions = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT && token != null) {
            UserDefinedFunctionMetaData function = UserDefinedFunctionMetaData.fromXContent(parser);
            functions.add(function);
        }
        return new UserDefinedFunctionsMetaData(functions.toArray(new UserDefinedFunctionMetaData[functions.size()]));
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
        return functions.equals(that.functions);
    }

}
