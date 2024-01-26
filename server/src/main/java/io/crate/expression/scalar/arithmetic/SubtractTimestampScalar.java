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

package io.crate.expression.scalar.arithmetic;

import java.util.List;

import org.joda.time.Period;
import org.joda.time.PeriodType;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.NodeContext;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class SubtractTimestampScalar extends Scalar<Period, Object> {

    public static void register(ScalarFunctionModule module) {
        for (var timestampType : List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
            module.register(
                Signature.scalar(
                    ArithmeticFunctions.Names.SUBTRACT,
                    timestampType.getTypeSignature(),
                    timestampType.getTypeSignature(),
                    DataTypes.INTERVAL.getTypeSignature()
                ),
                SubtractTimestampScalar::new
            );
        }
    }

    public SubtractTimestampScalar(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Period evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        Long start = (Long) args[1].value();
        Long end = (Long) args[0].value();
        if (end == null || start == null) {
            return null;
        }
        return new Period(end - start).normalizedStandard(PeriodType.yearMonthDayTime());
    }
}
