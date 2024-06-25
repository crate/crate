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

package io.crate.expression.symbol;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.format.Style;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class Aggregation implements Symbol {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Aggregation.class);

    private final Signature signature;
    private final DataType<?> boundSignatureReturnType;
    private final List<Symbol> inputs;
    private final DataType<?> valueType;
    private final Symbol filter;

    public Aggregation(Signature signature, DataType<?> valueType, List<Symbol> inputs) {
        this(signature,
            valueType,
            valueType,
            inputs,
            Literal.BOOLEAN_TRUE
        );
    }

    public Aggregation(Signature signature,
                       DataType<?> boundSignatureReturnType,
                       DataType<?> valueType,
                       List<Symbol> inputs,
                       Symbol filter) {
        requireNonNull(inputs, "inputs must not be null");
        requireNonNull(filter, "filter must not be null");

        this.valueType = valueType;
        this.signature = signature;
        this.boundSignatureReturnType = boundSignatureReturnType;
        this.inputs = inputs;
        this.filter = filter;
    }

    public Aggregation(StreamInput in) throws IOException {
        Signature generatedSignature = null;
        if (in.getVersion().before(Version.V_5_0_0)) {
            generatedSignature = Signature.readFromFunctionInfo(in);
        }
        valueType = DataTypes.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
            filter = Symbol.fromStream(in);
        } else {
            filter = Literal.BOOLEAN_TRUE;
        }
        inputs = Symbols.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (in.getVersion().before(Version.V_5_0_0)) {
                in.readBoolean();
            }
            signature = new Signature(in);
            boundSignatureReturnType = DataTypes.fromStream(in);
        } else {
            assert generatedSignature != null : "expecting a non-null generated signature";
            signature = generatedSignature;
            boundSignatureReturnType = valueType;
        }
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.AGGREGATION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return valueType;
    }

    public DataType<?> boundSignatureReturnType() {
        return boundSignatureReturnType;
    }

    @Nullable
    public Signature signature() {
        return signature;
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    public Symbol filter() {
        return filter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_0_0)) {
            signature.writeAsFunctionInfo(out, Symbols.typeView(inputs));
        }
        DataTypes.toStream(valueType, out);
        if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
            Symbol.toStream(filter, out);
        }
        Symbols.toStream(inputs, out);
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            if (out.getVersion().before(Version.V_5_0_0)) {
                out.writeBoolean(true);
            }
            signature.writeTo(out);
            DataTypes.toStream(boundSignatureReturnType, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Aggregation that &&
            Objects.equals(signature, that.signature) &&
            Objects.equals(inputs, that.inputs) &&
            Objects.equals(valueType, that.valueType) &&
            Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        int result = signature.hashCode();
        result = 31 * result + inputs.hashCode();
        result = 31 * result + valueType.hashCode();
        result = 31 * result + filter.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        StringBuilder sb = new StringBuilder(signature.getName().name())
            .append("(");
        for (int i = 0; i < inputs.size(); i++) {
            sb.append(inputs.get(i).toString(style));
            if (i + 1 < inputs.size()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + signature.ramBytesUsed()
            + boundSignatureReturnType.ramBytesUsed()
            + inputs.stream().mapToLong(Symbol::ramBytesUsed).sum()
            + valueType.ramBytesUsed()
            + filter.ramBytesUsed();
    }
}
