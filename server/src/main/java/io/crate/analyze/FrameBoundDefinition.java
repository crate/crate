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

package io.crate.analyze;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.FrameBound.Type;

public class FrameBoundDefinition implements Writeable {

    private final Type type;
    private final Symbol value;

    public FrameBoundDefinition(Type type, Symbol value) {
        assert type != null : "type must not be null";
        assert value != null : "value must not be null";
        this.type = type;
        this.value = value;
    }

    public FrameBoundDefinition(StreamInput in) throws IOException {
        type = in.readEnum(FrameBound.Type.class);
        if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
            value = Symbol.fromStream(in);
        } else {
            Symbol val = Symbol.nullableFromStream(in);
            value = val == null ? Literal.NULL : val;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
        if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
            Symbol.toStream(value, out);
        } else {
            Symbol.nullableToStream(value, out);
        }
    }

    public Type type() {
        return type;
    }

    public Symbol value() {
        return value;
    }

    public FrameBoundDefinition map(Function<? super Symbol, ? extends Symbol> mapper) {
        Symbol newValue = mapper.apply(value);
        if (newValue == value) {
            return this;
        } else {
            return new FrameBoundDefinition(type, newValue);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrameBoundDefinition that = (FrameBoundDefinition) o;
        return type == that.type &&
               Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return "FrameBound{" +
               "type=" + type +
               ", value=" + value +
               '}';
    }
}
