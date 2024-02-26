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

package io.crate.expression.scalar.arithmetic;


import static io.crate.metadata.functions.Signature.scalar;

import java.util.EnumSet;
import java.util.Random;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctions;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class RandomFunction extends Scalar<Double, Void> {

    public static final String NAME = "random";

    public static void register(ScalarFunctions module) {
        module.register(
            scalar(
                NAME,
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeatures(EnumSet.of(Feature.NON_NULLABLE)),
            RandomFunction::new
        );
    }

    private final Random random = new Random();

    public RandomFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        /* There is no evaluation here, so the function is executed
           per row. Else every row would contain the same random value*/
        assert symbol.arguments().size() == 0 : "function's number of arguments must be 0";
        return symbol;
    }


    @Override
    @SafeVarargs
    public final Double evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Void> ... args) {
        assert args.length == 0 : "number of args must be 0";
        return this.random.nextDouble();
    }
}
