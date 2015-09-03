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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;


public class FunctionInfo implements Comparable<FunctionInfo>, Streamable {

    private FunctionIdent ident;
    private DataType returnType;
    private Type type;
    private boolean deterministic;


    public enum Type {
        SCALAR,
        AGGREGATE,
        PREDICATE
    }

    public FunctionInfo() {

    }

    public FunctionInfo(FunctionIdent ident, DataType returnType) {
        this(ident, returnType, Type.SCALAR);
    }

    public FunctionInfo(FunctionIdent ident, DataType returnType, Type type) {
        this(ident, returnType, type, true);
    }

    public FunctionInfo(FunctionIdent ident, DataType returnType, Type type, boolean deterministic) {
        this.ident = ident;
        this.returnType = returnType;
        this.type = type;
        this.deterministic = deterministic;
    }

    public FunctionIdent ident() {
        return ident;
    }

    public Type type() {
        return type;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public DataType returnType() {
        return returnType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionInfo that = (FunctionInfo) o;
        return Objects.equal(deterministic, that.deterministic) &&
                Objects.equal(ident, that.ident) &&
                Objects.equal(returnType, that.returnType) &&
                Objects.equal(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, returnType, type, deterministic);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("ident", ident)
                .add("returnType", returnType)
                .add("deterministic", deterministic)
                .toString();
    }

    @Override
    public int compareTo(FunctionInfo o) {
        return ComparisonChain.start()
                .compare(type, o.type)
                .compare(ident, o.ident)
                .compare(returnType, o.returnType)
                .compare(deterministic, o.deterministic)
                .result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new FunctionIdent();
        ident.readFrom(in);

        returnType = DataTypes.fromStream(in);
        type = Type.values()[in.readVInt()];
        deterministic = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataTypes.toStream(returnType, out);
        out.writeVInt(type.ordinal());
        out.writeBoolean(deterministic);
    }
}
