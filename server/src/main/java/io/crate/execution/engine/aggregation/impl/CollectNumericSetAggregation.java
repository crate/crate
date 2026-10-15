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

import java.util.List;

import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/// Same semantics as `collect_set`, but limited to numeric types whose distinct values can be read
/// from the BKD points index instead of from documents.
/// See `LooseIndexScan#tryCollectNumericSet` for when that happens.
public final class CollectNumericSetAggregation extends CollectSetAggregation {

    public static final String NAME = "collect_numeric_set";

    private static final List<DataType<?>> SUPPORTED_TYPES = List.of(
        DataTypes.BYTE,
        DataTypes.SHORT,
        DataTypes.INTEGER,
        DataTypes.LONG,
        DataTypes.FLOAT,
        DataTypes.DOUBLE,
        DataTypes.DATE,
        DataTypes.TIMESTAMPZ,
        DataTypes.TIMESTAMP
    );

    public static void register(Functions.Builder builder) {
        for (DataType<?> supportedType : SUPPORTED_TYPES) {
            var returnType = new ArrayType<>(supportedType);
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(supportedType.getTypeSignature())
                    .returnType(returnType.getTypeSignature())
                    .features(Feature.DETERMINISTIC)
                    .build(),
                CollectNumericSetAggregation::new
            );
        }
    }

    private CollectNumericSetAggregation(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }
}
