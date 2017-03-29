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

import com.google.common.collect.ImmutableList;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DropUserDefinedFunctionRequest extends MasterNodeRequest<CreateUserDefinedFunctionRequest> {

    private String name;
    private String schema;
    private List<DataType> argumentTypes = ImmutableList.of();
    private boolean ifExists;

    public DropUserDefinedFunctionRequest() {
    }

    public DropUserDefinedFunctionRequest(String schema, String name, List<DataType> argumentTypes, boolean ifExists) {
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

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    boolean ifExists() {
        return ifExists;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name == null) {
            return ValidateActions.addValidationError("name is missing", null);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        schema = in.readString();
        name = in.readString();

        int n = in.readVInt();
        if (n > 0) {
            argumentTypes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                argumentTypes.add(DataTypes.fromStream(in));
            }
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
