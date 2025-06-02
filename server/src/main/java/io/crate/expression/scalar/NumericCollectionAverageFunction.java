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

package io.crate.expression.scalar;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class NumericCollectionAverageFunction extends Scalar<BigDecimal, List<BigDecimal>> {

    public static final String NAME = "collection_avg";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(new ArrayType<>(DataTypes.NUMERIC).getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            NumericCollectionAverageFunction::new
        );
    }

    private NumericCollectionAverageFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public BigDecimal evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<BigDecimal>>... args) {
        List<BigDecimal> values = args[0].value();
        if (values == null) {
            return null;
        }
        BigDecimal sum = new BigDecimal(0);
        long count = 0;
        for (BigDecimal value : values) {
            sum = sum.add(value) ;
            count++;
        }
        if (count > 0) {
            return sum.divide(new BigDecimal(count), MathContext.DECIMAL128);
        } else {
            return null;
        }
    }
}
