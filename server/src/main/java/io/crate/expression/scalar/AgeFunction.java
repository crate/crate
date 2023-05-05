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

package io.crate.expression.scalar;

import static io.crate.metadata.functions.Signature.scalar;

import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;
import io.crate.types.IntervalType;

public class AgeFunction extends Scalar<Period, Object> {

    private static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "age");

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                NAME,
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            AgeFunction::new
        );

        module.register(
            scalar(
                NAME,
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            AgeFunction::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public AgeFunction(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public Period evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        /*
         We cannot create Period from millis difference
         since sometimes we need to treat different millis range as same interval.
         Postgres treats age between first days of the sequential months as
         '1 month' despite on possibly different 28, 30, 31 days/millis range.
         */
        assert args.length > 0 && args.length < 3 : "Invalid number of arguments";

        var arg1 = args[0].value();
        if (arg1 == null) {
            return null;
        }
        if (args.length == 1) {
            long curDateMillis = txnCtx.currentInstant().toEpochMilli();
            curDateMillis = curDateMillis - curDateMillis % 86400000; // current_date at midnight, similar to CurrentDateFunction implementation.
            return IntervalType.subtractTimestamps(curDateMillis, (long) arg1);
        } else {
            var arg2 = args[1].value();
            if (arg2 == null) {
                return null;
            }
            return IntervalType.subtractTimestamps((long) arg1, (long) arg2);
        }
    }
}
