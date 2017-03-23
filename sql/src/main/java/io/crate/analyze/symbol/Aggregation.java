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

package io.crate.analyze.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class Aggregation extends Symbol {

    public Aggregation(StreamInput in) throws IOException {
        functionInfo = new FunctionInfo();
        functionInfo.readFrom(in);

        mode = Mode.readFrom(in);

        valueType = DataTypes.fromStream(in);
        inputs = Symbols.listFromStream(in);
    }

    public enum Mode {
        ITER_PARTIAL {
            @Override
            public DataType returnType(AggregationFunction function) {
                return function.partialType();
            }
        },
        ITER_FINAL,
        PARTIAL_FINAL;

        private static final List<Mode> VALUES = ImmutableList.copyOf(values());

        public DataType returnType(AggregationFunction function) {
            return function.info().returnType();
        }

        static void writeTo(Mode step, StreamOutput out) throws IOException {
            out.writeVInt(step.ordinal());
        }

        static Mode readFrom(StreamInput in) throws IOException {
            return VALUES.get(in.readVInt());
        }
    }

    private FunctionInfo functionInfo;
    private List<Symbol> inputs;
    private DataType valueType;
    private Mode mode;

    public Aggregation(FunctionInfo functionInfo, DataType valueType, List<Symbol> inputs, Mode mode) {
        Preconditions.checkNotNull(inputs, "inputs are null");

        this.valueType = valueType;
        this.functionInfo = functionInfo;
        this.inputs = inputs;
        this.mode = mode;
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
    public DataType valueType() {
        return valueType;
    }

    public FunctionIdent functionIdent() {
        return functionInfo.ident();
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        functionInfo.writeTo(out);

        Mode.writeTo(mode, out);
        DataTypes.toStream(valueType, out);
        Symbols.toStream(inputs, out);
    }
}
