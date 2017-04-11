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

package io.crate.operation.scalar.geo;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.UnaryScalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class GeoHashFunction {

    private static final List<DataType> SUPPORTED_INPUT_TYPES = ImmutableList.of(DataTypes.GEO_POINT, DataTypes.STRING, DataTypes.DOUBLE_ARRAY, DataTypes.UNDEFINED);

    private static void register(ScalarFunctionModule module, String name, Function<Object, BytesRef> func) {
        for (DataType inputType : SUPPORTED_INPUT_TYPES) {
            FunctionIdent ident = new FunctionIdent(name, Collections.singletonList(inputType));
            module.register(new UnaryScalar<>(new FunctionInfo(ident, DataTypes.STRING), func));
        }
    }

    public static void register(ScalarFunctionModule module) {
        register(module, "geohash", GeoHashFunction::getGeoHash);
    }


    private static BytesRef getGeoHash(Object value) {
        Double[] geoValue = GeoPointType.INSTANCE.value(value);
        return BytesRefs.toBytesRef(GeoHashUtils.stringEncode(geoValue[0], geoValue[1]));
    }
}

