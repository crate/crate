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

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DropUserDefinedFunctionRequest extends MasterNodeRequest<DropUserDefinedFunctionRequest> {

    private final String name;
    private final String schema;
    private final List<DataType<?>> argumentTypes;
    private final boolean ifExists;

    public DropUserDefinedFunctionRequest(String schema, String name, List<DataType<?>> argumentTypes, boolean ifExists) {
        this.schema = schema;
        this.name = name;
        this.argumentTypes = argumentTypes;
        this.ifExists = ifExists;
    }

    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public List<DataType<?>> argumentTypes() {
        return argumentTypes;
    }

    boolean ifExists() {
        return ifExists;
    }

    public DropUserDefinedFunctionRequest(StreamInput in) throws IOException {
        super(in);
        schema = in.readString();
        name = in.readString();
        int n = in.readVInt();
        if (n > 0) {
            argumentTypes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                argumentTypes.add(DataTypes.fromStream(in));
            }
        } else {
            argumentTypes = List.of();
        }
        ifExists = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(schema);
        out.writeString(name);
        int n = argumentTypes.size();
        out.writeVInt(n);
        for (int i = 0; i < n; i++) {
            DataTypes.toStream(argumentTypes.get(i), out);
        }
        out.writeBoolean(ifExists);
    }
}
