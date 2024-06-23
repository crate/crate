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

import java.math.BigDecimal;

import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public final class NegateFunctions {

    public static final String NAME = "_negate";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.scalar(
                    NAME,
                    Scalar.Feature.NULLABLE,
                    TypeSignature.parse("double precision"),
                    TypeSignature.parse("double precision")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.DOUBLE, x -> x * -1)
        );
        builder.add(
            Signature.scalar(
                    NAME,
                    Scalar.Feature.NULLABLE,
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.FLOAT, x -> x * -1)
        );
        builder.add(
            Signature.scalar(
                NAME,
                Scalar.Feature.NULLABLE,
                TypeSignature.parse("integer"),
                TypeSignature.parse("integer")
            ).withFeature(Scalar.Feature.DETERMINISTIC).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.INTEGER, x -> x * -1)
        );
        builder.add(
            Signature.scalar(
                    NAME,
                    Scalar.Feature.NULLABLE,
                    TypeSignature.parse("bigint"),
                    TypeSignature.parse("bigint")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.LONG, x -> x * -1)
        );
        builder.add(
            Signature.scalar(
                    NAME,
                    Scalar.Feature.NULLABLE,
                    TypeSignature.parse("smallint"),
                    TypeSignature.parse("smallint")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.SHORT, x -> (short) (x * -1))
        );
        builder.add(
            Signature.scalar(
                    NAME,
                    Scalar.Feature.NULLABLE,
                    DataTypes.NUMERIC.getTypeSignature(),
                    DataTypes.NUMERIC.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withForbiddenCoercion(),
            (signature, boundSignature) ->
                new UnaryScalar<>(signature, boundSignature, DataTypes.NUMERIC, BigDecimal::negate)
        );
    }
}
