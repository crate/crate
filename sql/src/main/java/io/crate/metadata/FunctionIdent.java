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
import com.google.common.collect.Ordering;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FunctionIdent implements Comparable<FunctionIdent>, Streamable {

    private String name;
    private List<DataType> argumentTypes;
    private boolean distinct;

    public FunctionIdent() {

    }

    public FunctionIdent(String name, List<DataType> argumentTypes) {
        this(name, argumentTypes, false);
    }

    public FunctionIdent(String name, List<DataType> argumentTypes, boolean distinct) {
        this.name = name;
        this.argumentTypes = argumentTypes;
        this.distinct = distinct;
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    public String name() {
        return name;
    }

    public boolean isDistinct() {
        return distinct;
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
        return Objects.equal(name, o.name) &&
                Objects.equal(argumentTypes, o.argumentTypes) &&
                distinct == o.distinct;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, argumentTypes, distinct);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("argumentTypes", argumentTypes)
                .add("distinct", distinct)
                .toString();
    }

    @Override
    public int compareTo(FunctionIdent o) {
        return ComparisonChain.start()
                .compare(name, o.name)
                .compare(argumentTypes, o.argumentTypes, Ordering.<DataType>natural().lexicographical())
                .compareFalseFirst(distinct, o.distinct)
                .result();
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int numTypes = in.readVInt();
        argumentTypes = new ArrayList<>(numTypes);

        for (int i = 0; i < numTypes; i++) {
            argumentTypes.add(DataType.fromStream(in));
        }

        distinct = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(argumentTypes.size());

        for (DataType argumentType : argumentTypes) {
            DataType.toStream(argumentType, out);
        }

        out.writeBoolean(distinct);
    }
}
