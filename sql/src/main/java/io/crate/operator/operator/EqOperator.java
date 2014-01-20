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

package io.crate.operator.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;

import java.util.Objects;


public class EqOperator implements Operator {

    public static final String NAME = "op_eq";
    private final FunctionInfo info;

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(
                new EqOperator(new FunctionInfo(
                        new FunctionIdent(NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)),
                        DataType.BOOLEAN,
                        false)
                )
        );
        module.registerOperatorFunction(
                new EqOperator(new FunctionInfo(
                        new FunctionIdent(NAME, ImmutableList.of(DataType.DOUBLE, DataType.DOUBLE)),
                        DataType.BOOLEAN,
                        false)
                )
        );
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol optimizeSymbol(Symbol symbol) {
        Preconditions.checkNotNull(symbol);
        Preconditions.checkArgument(symbol.symbolType() == SymbolType.FUNCTION);
        Function function = (Function)symbol;
        Preconditions.checkArgument(function.arguments().size() == 2);

        return new BooleanLiteral(
                Objects.equals(function.arguments().get(0), function.arguments().get(1)));
    }

    EqOperator(FunctionInfo info) {
        this.info = info;
    }
}
