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

package io.crate.expression.scalar.geo;

import static io.crate.metadata.functions.Signature.scalar;

import org.locationtech.spatial4j.shape.Point;

import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;

public final class CoordinateFunction {

    public static void register(Functions.Builder module) {
        module.add(
            scalar(
                "latitude",
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.GEO_POINT,
                    CoordinateFunction::getLatitude
                )
        );
        module.add(
            scalar(
                "longitude",
                DataTypes.GEO_POINT.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.GEO_POINT,
                    CoordinateFunction::getLongitude
                )
        );
    }


    private static Double getLatitude(Point point) {
        return point.getY();
    }

    private static Double getLongitude(Point point) {
        return point.getX();
    }
}
