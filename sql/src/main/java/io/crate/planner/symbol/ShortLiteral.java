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

import com.google.common.base.Preconditions;
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ShortLiteral extends NumberLiteral<Integer, ShortLiteral> {
    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            return new ShortLiteral();
        }
    };

    private short value;

    public ShortLiteral() {}

    public ShortLiteral(long value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("invalid short literal %s", value));
        }
        this.value = (short)value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShortLiteral that = (ShortLiteral) o;

        if (value != that.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public int compareTo(ShortLiteral o) {
        Preconditions.checkNotNull(o);
        return Integer.signum(Short.compare(value, o.value));
    }

    @Override
    public Integer value() {
        return (int)value;
    }

    @Override
    public DataType valueType() {
        return DataType.SHORT;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.SHORT_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitShortLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readShort();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeShort(value);
    }
}
