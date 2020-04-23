/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiFunction;

public class IntervalArithmeticScalar extends Scalar<Period, Object> {

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                ArithmeticFunctions.Names.ADD,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature()
            ),
            (signature, args) ->
                new IntervalArithmeticScalar("+", ArithmeticFunctions.Names.ADD, signature)
        );
        module.register(
            Signature.scalar(
                ArithmeticFunctions.Names.SUBTRACT,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature()
            ),
            (signature, args) ->
                new IntervalArithmeticScalar("-", ArithmeticFunctions.Names.SUBTRACT, signature)
        );
    }

    private final FunctionInfo info;
    private final Signature signature;
    private final BiFunction<Period, Period, Period> operation;

    IntervalArithmeticScalar(String operator, String name, Signature signature) {
        info = new FunctionInfo(
            new FunctionIdent(
                name,
                List.of(DataTypes.INTERVAL, DataTypes.INTERVAL)),
            DataTypes.INTERVAL);
        this.signature = signature;

        switch (operator) {
            case "+":
                operation = Period::plus;
                break;
            case "-":
                operation = Period::minus;
                break;
            default:
                operation = (a, b) -> {
                    throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                };
        }
    }

    @Override
    public FunctionInfo info() {
        return this.info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Period evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        Period fst = (Period) args[0].value();
        Period snd = (Period) args[1].value();

        if (fst == null || snd == null) {
            return null;
        }
        return operation.apply(fst, snd);
    }
}
