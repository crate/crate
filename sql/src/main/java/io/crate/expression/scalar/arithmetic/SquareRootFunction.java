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

package io.crate.expression.scalar.arithmetic;

import com.google.common.base.Preconditions;
import io.crate.expression.scalar.DoubleScalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public final class SquareRootFunction {

    public static final String NAME = "sqrt";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private static double sqrt(double value) {
        Preconditions.checkArgument(value >= 0, "cannot take square root of a negative number");
        return Math.sqrt(value);
    }

    private static class Resolver implements FunctionResolver {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType argType = dataTypes.get(0);
            return new DoubleScalar(NAME, argType, SquareRootFunction::sqrt);
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> symbols) {
            return FuncParams.SINGLE_NUMERIC.match(symbols);
        }
    }
}
