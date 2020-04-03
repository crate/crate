/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class CollectionAverageFunction extends Scalar<Double, List<Object>> {

    public static final String NAME = "collection_avg";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                DataTypes.DOUBLE.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, argumentTypes) ->
                new CollectionAverageFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.DOUBLE),
                    signature
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;

    private CollectionAverageFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @Override
    public Double evaluate(TransactionContext txnCtx, Input<List<Object>>... args) {
        List<Object> values = args[0].value();
        if (values == null) {
            return null;
        }
        double sum = 0;
        long count = 0;
        for (Object value : values) {
            sum += ((Number) value).doubleValue();
            count++;
        }
        if (count > 0) {
            return sum / count;
        } else {
            return null;
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }
}
