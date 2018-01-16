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

package io.crate.execution.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;

import java.util.function.DoubleUnaryOperator;

/**
 * Scalar implementation that wraps another function: f(double) -> double
 * <br />
 * null values will result in null as output
 *
 */
public final class DoubleScalar extends Scalar<Double, Number> {

    private final FunctionInfo info;
    private final DoubleUnaryOperator func;

    public DoubleScalar(FunctionInfo info, DoubleUnaryOperator func) {
        this.info = info;
        this.func = func;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @SafeVarargs
    @Override
    public final Double evaluate(Input<Number>... args) {
        assert args.length == 1 : "DoubleScalar expects exactly 1 argument, got: " + args.length;
        Number value = args[0].value();
        if (value == null) {
            return null;
        }
        return func.applyAsDouble(value.doubleValue());
    }
}
