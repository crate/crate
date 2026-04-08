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

package io.crate.execution.engine.aggregation.impl;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

import java.math.BigDecimal;

/**
 * NumericMaxAggregation implements the `max()` aggregation function for the {@link NumericType},
 * by taking advantage of the column store.
 */
public class NumericMaxAggregation extends NumericMinMaxBase {

    public static final String NAME = "max";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.AGGREGATE)
        .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
        .returnType(DataTypes.NUMERIC.getTypeSignature())
        .features(Scalar.Feature.DETERMINISTIC)
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, NumericMaxAggregation::new);
    }

    @VisibleForTesting
    private NumericMaxAggregation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    protected void setNewStateValue(BigDecimalValueWrapper state, BigDecimal value, RamAccounting ramAccounting) {
        if (value == null) {
            return;
        }

        BigDecimal newValue;
        long sizeDiff;

        if (state.value() == null) {
            newValue = value;
            sizeDiff = NumericType.size(newValue);
        } else {
            newValue = state.value().max(value);
            sizeDiff = NumericType.sizeDiff(newValue, state.value());
        }

        state.setValue(newValue);
        ramAccounting.addBytes(sizeDiff);
    }

    @Override
    public BigDecimalValueWrapper reduce(RamAccounting ramAccounting,
                                         BigDecimalValueWrapper state1,
                                         BigDecimalValueWrapper state2) {

        return state1.max(state2);
    }


}
