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

import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * This class implements the pg_catalog.age function
 * Usage example:
 * <pre>
 *    SELECT pg_catalog.age(timestamp '2021-11-06')
 *
 * </pre>
 *
 */
public class AgeFunction extends Scalar<Long, Object> {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "age");
    private final Signature signature;
    private final Signature boundSignature;


    /**
     * Registers the pg_catalog function in the {@link ScalarFunctionModule}.
     * It takes as input a TIMESTAMP  or two TIMESTAMP
     * and produces a LONG
     * @param module the {@link ScalarFunctionModule}
     */
    public static void register(ScalarFunctionModule module) {
        List<DataType<?>> supportedTimestampTypes = List.of(
            DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP, DataTypes.LONG);
        for (DataType<?> dataType : supportedTimestampTypes) {

            module.register(
                Signature.scalar(
                    NAME,
                    dataType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()
                ), AgeFunction::new
            );

            module.register(
                Signature.scalar(
                    NAME,
                    dataType.getTypeSignature(),
                    dataType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()
                ),
                AgeFunction::new
            );

        }
    }


    public AgeFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }


    @Override
    public Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {

        assert args.length >= 1 && args.length <= 2 :
            "Signature must ensure that there are one or two arguments";

        if (checkNullInput(args))
            return null;

        Object earliestValue = args[args.length - 1].value();
        Long timestampEarliest = DataTypes.LONG.sanitizeValue(earliestValue);
        Long timestampNewest = getNewestDateValue(txnCtx, args);
        Long duration = timestampNewest.longValue() - timestampEarliest.longValue();

        return duration;
    }


    private boolean checkNullInput(Input<Object>[] args) {

        if (args[0] == null) {
            return true;
        }
        if (args.length == 2 && args[1] == null) {
            return true;
        }

        return false;
    }


    private Long getNewestDateValue(TransactionContext txnCtx, Input<Object>... args) {

        Long timestampNewest;

        if (args.length == 2) {
            Object newestValue = args[0].value();
            timestampNewest = DataTypes.LONG.sanitizeValue(newestValue);
        } else {
            timestampNewest = getTodayMidnightAtUTC(txnCtx);
        }

        return timestampNewest;
    }


    private Long getTodayMidnightAtUTC(TransactionContext txnCtx) {

        return ChronoUnit.MILLIS.between(Instant.EPOCH,txnCtx.currentInstant().now().truncatedTo(ChronoUnit.DAYS));
    }


    @Override
    public Signature signature() {
        return signature;
    }


    @Override
    public Signature boundSignature() {
        return boundSignature;
    }
}
