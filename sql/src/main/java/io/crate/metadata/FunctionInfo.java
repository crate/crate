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


// PRESTOBORROW

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class FunctionInfo implements Comparable<FunctionInfo>, Streamable {

    private FunctionIdent ident;
    private DataType returnType;
    private boolean isAggregate;
    private boolean deterministic;

    public FunctionInfo() {

    }

    public FunctionInfo(FunctionIdent ident, DataType returnType) {
        this(ident, returnType, false);
    }

    public FunctionInfo(FunctionIdent ident, DataType returnType, boolean isAggregate) {
        this.ident = ident;
        this.returnType = returnType;
        this.isAggregate = isAggregate;
        this.deterministic = true;
    }

    public FunctionIdent ident() {
        return ident;
    }

    public boolean isAggregate() {

        return isAggregate;
    }


    public DataType returnType() {
        return returnType;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionInfo o = (FunctionInfo) obj;
        return Objects.equal(isAggregate, o.isAggregate) &&
                Objects.equal(ident, o.ident) &&
                Objects.equal(returnType, o.returnType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(isAggregate, ident, returnType);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("isAggregate", isAggregate)
                .add("ident", ident)
                .add("returnType", returnType)
                .toString();
    }

    @Override
    public int compareTo(FunctionInfo o) {
        return ComparisonChain.start()
                .compareTrueFirst(isAggregate, o.isAggregate)
                .compare(ident, o.ident)
                .compare(returnType, o.returnType)
                .result();
    }

    public static Predicate<FunctionInfo> isAggregationPredicate() {
        return new Predicate<FunctionInfo>() {
            @Override
            public boolean apply(FunctionInfo functionInfo) {
                return functionInfo.isAggregate();
            }
        };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new FunctionIdent();
        ident.readFrom(in);

        returnType = DataType.fromStream(in);
        isAggregate = in.readBoolean();
        deterministic = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataType.toStream(returnType, out);
        out.writeBoolean(isAggregate);
        out.writeBoolean(deterministic);
    }
}
