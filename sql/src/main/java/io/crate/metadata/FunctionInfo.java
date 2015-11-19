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
    private boolean comparisonReplacementPossible;

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
        this(ident, returnType, type, true, false);
    }

    public FunctionInfo(FunctionIdent ident, DataType returnType, Type type, boolean deterministic, boolean comparisonReplacementPossible) {
        this.ident = ident;
        this.returnType = returnType;
        this.type = type;
        this.deterministic = deterministic;
        this.comparisonReplacementPossible = comparisonReplacementPossible;
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


    /**
     *  If this method returns <code>true</code>, for this function it is possible to replace the containing
     *  comparison while preserving the truth value for all used operators
     *  with or without an operator mapping.
     *
     *  It describes the following:
     *
     *  say we have a comparison-query like this:
     *
     *  col > 10.5
     *
     *  then a function f, for which comparions are replaceable, can be applied so
     *  that:
     *
     *  f(col) > f(10.5)
     *
     *  for all col for which col > 10.5 is true. Maybe > needs to be mapped to another
     *  operator, but this may not depend on the actual values used here.
     *
     *  Fun facts:
     *
     *      * This property holds for the = comparison operator for all functions f.
     *      * This property is transitive so if f and g are replaceable,
     *        then f(g(x)) also is
     *      * it is possible to replace:
     *
     *          col > 10.5
     *
     *        with:
     *
     *          f(col) > f(10.5)
     *
     *        for every operator (possibly mapped) and the query is still equivalent.
     *
     *  Example 1:
     *
     *      if f is defined as f(v) = v + 1
     *      then col + 1 > 11.5 must be true for all col > 10.5.
     *      This is indeed true.
     *
     *      So f is replaceable for >.
     *
     *      Fun fact: for f all comparison operators =, >, <, >=,<= are replaceable
     *
     *  Example 2 (a rounding function):
     *
     *      if f is defined as f(v) = ceil(v)
     *      then ceil(col) > 11 for all col > 10.5.
     *      But there is 10.8 for which f is 11 and
     *      11 > 11 is false.
     *
     *      Here a simple mapping of the operator will do the trick:
     *
     *          > -> >=
     *          < -> <=
     *
     *      So for f comparisons are replaceable using the mapping above.
     *
     *  Example 3:
     *
     *      if f is defined as f(v) = v % 5
     *      then col % 5 > 0.5 for all col > 10.5
     *      but there is 20 for which
     *      f is 0 and
     *      0 > 0.5 is false.
     *
     *      So for f comparisons cannot be replaced.
     *
     */
    public boolean comparisonReplacementPossible() {
        return comparisonReplacementPossible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionInfo that = (FunctionInfo) o;
        return Objects.equal(deterministic, that.deterministic) &&
                Objects.equal(ident, that.ident) &&
                Objects.equal(returnType, that.returnType) &&
                Objects.equal(type, that.type) &&
                Objects.equal(comparisonReplacementPossible, that.comparisonReplacementPossible);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, returnType, type, deterministic, comparisonReplacementPossible);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("ident", ident)
                .add("returnType", returnType)
                .add("deterministic", deterministic)
                .add("comparisonReplacementPossible", comparisonReplacementPossible)
                .toString();
    }

    @Override
    public int compareTo(FunctionInfo o) {
        return ComparisonChain.start()
                .compare(type, o.type)
                .compare(ident, o.ident)
                .compare(returnType, o.returnType)
                .compare(deterministic, o.deterministic)
                .compare(comparisonReplacementPossible, o.comparisonReplacementPossible)
                .result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new FunctionIdent();
        ident.readFrom(in);

        returnType = DataTypes.fromStream(in);
        type = Type.values()[in.readVInt()];
        deterministic = in.readBoolean();

        comparisonReplacementPossible = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataTypes.toStream(returnType, out);
        out.writeVInt(type.ordinal());
        out.writeBoolean(deterministic);
        out.writeBoolean(comparisonReplacementPossible);
    }
}
