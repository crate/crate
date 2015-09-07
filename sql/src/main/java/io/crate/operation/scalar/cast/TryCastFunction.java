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

package io.crate.operation.scalar.cast;

import com.google.common.collect.Iterables;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class TryCastFunction<R, T> extends Scalar<R, T> {

    public static final String NAME = "try_cast";
    private final FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    TryCastFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 1);
        return Scalar.evaluateIfLiterals(this, symbol);
    }

    @Override
    @Nullable
    public R evaluate(Input[] args) {
        assert args.length == 1;
        try {
            return (R) args[0].value();
        } catch (ClassCastException | IllegalArgumentException e) {
            return null;
        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new TryCastFunction<>(new FunctionInfo(new FunctionIdent(NAME, dataTypes),
                    Iterables.getOnlyElement(dataTypes)));
        }
    }
}
