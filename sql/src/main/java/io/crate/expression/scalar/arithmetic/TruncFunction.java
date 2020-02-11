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

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.function.Function;


public final class TruncFunction {

    public static final String NAME = "trunc";
    private static final FuncParams SIGNATURE = FuncParams
        .builder(Param.NUMERIC)
        .withVarArgs(Param.INTEGER)
        .limitVarArgOccurrences(1)
        .build();

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(SIGNATURE) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
                DataType argType = types.get(0);
                DataType returnType = DataTypes.getIntegralReturnType(argType);
                if (1 == types.size()) {
                    return new UnaryScalar<Number, Number>(NAME, argType, returnType, n -> {
                        double val = n.doubleValue();
                        Function<Double, Double> f = val >= 0 ? Math::floor : Math::ceil;
                        return (Number) returnType.value(f.apply(val));
                    });
                } else {
                    return new Scalar<Number, Number>() {

                        FunctionInfo info = new FunctionInfo(new FunctionIdent(
                            NAME, types), DataTypes.DOUBLE);

                        @Override
                        public FunctionInfo info() {
                            return info;
                        }

                        @Override
                        public Number evaluate(TransactionContext txnCtx, Input<Number>... args) {
                            Number n = args[0].value();
                            Number nd = args[1].value();
                            if (null == n || null == nd) {
                                return null;
                            }
                            double val = n.doubleValue();
                            int numDecimals = nd.intValue();
                            RoundingMode mode = val >= 0 ? RoundingMode.FLOOR : RoundingMode.CEILING;
                            return BigDecimal.valueOf(val).setScale(numDecimals, mode).doubleValue();
                        }
                    };
                }
            }
        });
    }
}
