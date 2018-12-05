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

import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

public final class CeilFunction {

    public static final String NAME = "ceil";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new FunctionResolver() {
            @Nullable
            @Override
            public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
                return FuncParams.SINGLE_NUMERIC.match(funcArgs);
            }

            @Override
            public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
                if (types.size() != 1) {
                    throw FunctionResolver.noSignatureMatch(NAME, types);
                }
                DataType argType = types.get(0);
                DataType returnType = DataTypes.getIntegralReturnType(argType);
                if (returnType == null) {
                    throw FunctionResolver.noSignatureMatch(NAME, types);
                }
                return new UnaryScalar<>(
                    NAME, argType, returnType, x -> returnType.value(Math.ceil(((Number) x).doubleValue())));
            }
        });
    }
}
