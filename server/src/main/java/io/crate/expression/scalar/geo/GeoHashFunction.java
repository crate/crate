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

import org.elasticsearch.common.geo.GeoHashUtils;
import org.locationtech.spatial4j.shape.Point;

import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;

public final class GeoHashFunction {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("geohash", FunctionType.SCALAR)
                .argumentTypes(DataTypes.GEO_POINT.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.NULLABLE)
                .build(),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.GEO_POINT,
                    GeoHashFunction::getGeoHash
                )
        );
    }

    private static String getGeoHash(Object value) {
        Point geoValue = GeoPointType.INSTANCE.sanitizeValue(value);
        return GeoHashUtils.stringEncode(geoValue.getX(), geoValue.getY());
    }
}

