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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class Aggregation extends Symbol {

    public static final SymbolFactory<Aggregation> FACTORY = new SymbolFactory<Aggregation>() {
        @Override
        public Aggregation newInstance() {
            return new Aggregation();
        }
    };

    public Aggregation() {

    }

    public static enum Step {
        ITER, PARTIAL, FINAL;

        static void writeTo(Step step, StreamOutput out) throws IOException {
            out.writeVInt(step.ordinal());
        }

        static Step readFrom(StreamInput in) throws IOException {
            return values()[in.readVInt()];
        }
    }

    private FunctionInfo functionInfo;
    private List<Symbol> inputs;
    private DataType valueType;
    private Step fromStep;
    private Step toStep;

    public static Aggregation partialAggregation(FunctionInfo functionInfo, DataType partialType, List<Symbol> inputs) {
        return new Aggregation(functionInfo, partialType, inputs, Step.ITER, Step.PARTIAL);
    }

    public static Aggregation finalAggregation(FunctionInfo functionInfo, List<Symbol> inputs, Step fromStep) {
        return new Aggregation(functionInfo, functionInfo.returnType(), inputs, fromStep, Step.FINAL);
    }

    private Aggregation(FunctionInfo functionInfo, DataType valueType, List<Symbol> inputs, Step fromStep, Step toStep) {
        Preconditions.checkNotNull(inputs, "inputs are null");
        assert fromStep != Step.FINAL : "Can't start from FINAL";
        this.valueType = valueType;
        this.functionInfo = functionInfo;
        this.inputs = inputs;
        this.fromStep = fromStep;
        this.toStep = toStep;
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

    public Step fromStep() {
        return fromStep;
    }


    public Step toStep() {
        return toStep;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        functionInfo = new FunctionInfo();
        functionInfo.readFrom(in);

        fromStep = Step.readFrom(in);
        toStep = Step.readFrom(in);

        valueType = DataTypes.fromStream(in);
        inputs = Symbols.listFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        functionInfo.writeTo(out);

        Step.writeTo(fromStep, out);
        Step.writeTo(toStep, out);

        DataTypes.toStream(valueType, out);
        Symbols.toStream(inputs, out);
    }
}
