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

package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FunctionIdent implements Comparable<FunctionIdent>, Streamable {

    private String schema;
    private String name;
    private List<DataType> argumentTypes;

    public FunctionIdent() {

    }

    public static FunctionIdent of(String name, DataType type1, DataType type2) {
        return new FunctionIdent(name, ImmutableList.of(type1, type2));
    }

    public FunctionIdent(@Nullable String schema, String name, List<DataType> argumentTypes) {
        this.schema = schema;
        this.name = name;
        this.argumentTypes = argumentTypes;
    }

    public FunctionIdent(String name, List<DataType> argumentTypes) {
        this.name = name;
        this.argumentTypes = argumentTypes;
    }

    public String schema() {
        return schema;
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionIdent o = (FunctionIdent) obj;
        return Objects.equal(schema, o.schema) &&
            Objects.equal(name, o.name) &&
            Objects.equal(argumentTypes, o.argumentTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema, name, argumentTypes);
    }

    @Override
    public String toString() {
        return "FunctionIdent{" +
            "schema='" + schema + '\'' +
            ", name='" + name + '\'' +
            ", argumentTypes=" + argumentTypes +
            '}';
    }

    @Override
    public int compareTo(FunctionIdent o) {
        return ComparisonChain.start()
            .compare(schema, o.schema)
            .compare(name, o.name)
            .compare(argumentTypes, o.argumentTypes, Ordering.<DataType>natural().lexicographical())
            .result();
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        schema = in.readOptionalString();
        name = in.readString();
        int numTypes = in.readVInt();
        argumentTypes = new ArrayList<>(numTypes);

        for (int i = 0; i < numTypes; i++) {
            argumentTypes.add(DataTypes.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(schema);
        out.writeString(name);
        out.writeVInt(argumentTypes.size());

        for (DataType argumentType : argumentTypes) {
            DataTypes.toStream(argumentType, out);
        }
    }
}
