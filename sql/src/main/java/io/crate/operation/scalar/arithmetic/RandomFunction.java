/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar.arithmetic;


import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.Random;

public class RandomFunction extends Scalar<Double, Void> {

    public static final String NAME = "random";

    protected static final FunctionInfo info = new FunctionInfo(
        new FunctionIdent(NAME, Collections.emptyList()), DataTypes.DOUBLE,
        FunctionInfo.Type.SCALAR, FunctionInfo.NO_FEATURES);

    private final Random random = new Random();

    public static void register(ScalarFunctionModule module) {
        module.register(new RandomFunction());
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        /* There is no evaluation here, so the function is executed
           per row. Else every row would contain the same random value*/
        assert symbol.arguments().size() == 0 : "function's number of arguments must be 0";
        return symbol;
    }


    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Double evaluate(Input[] args) {
        assert args.length == 0 : "number of args must be 0";
        return this.random.nextDouble();
    }
}
