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

package io.crate.planner.symbol;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Value extends DataTypeSymbol {

    public static final SymbolFactory<Value> FACTORY = new SymbolFactory<Value>() {
        @Override
        public Value newInstance() {
            return new Value();
        }
    };

    private DataType type;

    public Value(DataType type) {
        this.type = type;
    }

    public Value() {

    }

    public DataType valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.VALUE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitValue(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        type = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Value value = (Value) o;

        if (type != value.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return type != null ? type.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Value{" +
                "type=" + type +
                '}';
    }
}
