/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class SetLiteral<ValueType, LiteralType extends Literal> extends Literal<Set<ValueType>, Set<LiteralType>> {

    protected Set<ValueType> values;
    protected Set<LiteralType> literals;

    public SetLiteral(Set<LiteralType> literals) {
        Preconditions.checkNotNull(literals);
        this.literals = literals;
        values = new HashSet<>(literals.size());
        for (Literal l : literals) {
            values.add((ValueType)l.value());
        }
    }

    public SetLiteral() {}

    @Override
    public Set<ValueType> value() {
        return values;
    }

    public Set<LiteralType> literals() {
        return literals;
    }

    @Override
    public int compareTo(Set<LiteralType> o) {
        return 0;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        literals = (Set<LiteralType>) in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(literals);
    }

    public static SetLiteral forType(DataType type, Set<Literal> literals) {
        switch (type) {
            case BYTE:
            case BYTE_SET:
                return new IntegerSetLiteral((Set)literals);
            case SHORT:
            case SHORT_SET:
                return new IntegerSetLiteral((Set)literals);
            case INTEGER:
            case INTEGER_SET:
                return new IntegerSetLiteral((Set)literals);
//            case TIMESTAMP:
//            case TIMESTAMP_SET:
            case LONG:
            case LONG_SET:
                return new LongSetLiteral((Set)literals);
//            case FLOAT:
//            case FLOAT_SET:
//                return new FloatSetLiteral((Set)literals);
//            case DOUBLE:
//            case DOUBLE_SET:
//                return new DoubleSetLiteral((Set)literals);
//            case BOOLEAN:
//            case BOOLEAN_SET:
//                return new BooleanSetLiteral((Set)literals);
//            case IP:
//            case STRING:
//                return new StringSetLiteral((Set)literals);
//            case OBJECT:
//                break;
            case NOT_SUPPORTED:
                throw new UnsupportedOperationException();
        }

        return null;
    }

}
