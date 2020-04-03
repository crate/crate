/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.locationtech.spatial4j.shape.Point;

import java.util.List;

import static io.crate.metadata.functions.Signature.scalar;

public final class GeoHashFunction {

    private static final List<DataType<?>> SUPPORTED_INPUT_TYPES =
        List.of(DataTypes.GEO_POINT, DataTypes.STRING, DataTypes.DOUBLE_ARRAY);

    public static void register(ScalarFunctionModule module) {
        for (DataType<?> inputType : SUPPORTED_INPUT_TYPES) {
            module.register(
                scalar(
                    "geohash",
                    inputType.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ),
                (signature, args) ->
                    new UnaryScalar<>(
                        "geohash",
                        signature,
                        inputType,
                        DataTypes.STRING,
                        GeoHashFunction::getGeoHash
                    )
            );
        }
    }


    private static String getGeoHash(Object value) {
        Point geoValue = GeoPointType.INSTANCE.value(value);
        return GeoHashUtils.stringEncode(geoValue.getX(), geoValue.getY());
    }
}

