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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
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
    private Step fromStep;
    private Step toStep;

    public Aggregation(FunctionInfo functionInfo, List<Symbol> inputs, Step fromStep, Step toStep) {
        Preconditions.checkNotNull(inputs, "inputs are null");
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
        if (toStep == Step.PARTIAL) {
            return DataTypes.UNDEFINED;
        }
        return functionInfo.returnType();
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

        int numInputs = in.readVInt();
        inputs = new ArrayList<>(numInputs);
        for (int i = 0; i < numInputs; i++) {
            inputs.add(Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        functionInfo.writeTo(out);

        Step.writeTo(fromStep, out);
        Step.writeTo(toStep, out);

        out.writeVInt(inputs.size());
        for (Symbol input : inputs) {
            Symbol.toStream(input, out);
        }
    }
}
