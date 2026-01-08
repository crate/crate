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

package io.crate.expression.scalar.postgres;

import java.util.EnumSet;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;


public class PgSleepFunction extends Scalar<Boolean, Double> {

    public static final String NAME = "pg_sleep";

    public PgSleepFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(Feature.NOTNULL))
                .build(),
            PgSleepFunction::new
        );
    }

    @SafeVarargs
    @Override
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Double>... args) {
        assert args.length == 1 : NAME + " expects exactly 1 argument, got " + args.length;

        Double duration = args[0].value();
        if (duration == null) {
            return true;
        }
        try {
            Thread.sleep((long)(duration.doubleValue() * 1000.0));
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
