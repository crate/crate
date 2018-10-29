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

import com.google.common.collect.ImmutableList;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import org.elasticsearch.common.geo.GeoHashUtils;

import java.util.List;
import java.util.function.Function;

public final class GeoHashFunction {

    private static final List<DataType> SUPPORTED_INPUT_TYPES = ImmutableList.of(DataTypes.GEO_POINT, DataTypes.STRING, DataTypes.DOUBLE_ARRAY);

    private static void register(ScalarFunctionModule module, String name, Function<Object, String> func) {
        for (DataType inputType : SUPPORTED_INPUT_TYPES) {
            module.register(new UnaryScalar<>(name, inputType, DataTypes.STRING, func));
        }
    }

    public static void register(ScalarFunctionModule module) {
        register(module, "geohash", GeoHashFunction::getGeoHash);
    }


    private static String getGeoHash(Object value) {
        Double[] geoValue = GeoPointType.INSTANCE.value(value);
        return GeoHashUtils.stringEncode(geoValue[0], geoValue[1]);
    }
}

