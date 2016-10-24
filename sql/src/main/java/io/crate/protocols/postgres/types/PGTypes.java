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

package io.crate.protocols.postgres.types;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.collect.ImmutableMap;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class PGTypes {

    private static final Map<DataType, PGType> CRATE_TO_PG_TYPES = ImmutableMap.<DataType, PGType>builder()
        .put(DataTypes.BYTE, CharType.INSTANCE)
        .put(DataTypes.STRING, VarCharType.INSTANCE)
        .put(DataTypes.BOOLEAN, BooleanType.INSTANCE)
        .put(DataTypes.OBJECT, JsonType.INSTANCE)
        .put(DataTypes.SHORT, SmallIntType.INSTANCE)
        .put(DataTypes.INTEGER, IntegerType.INSTANCE)
        .put(DataTypes.LONG, BigIntType.INSTANCE)
        .put(DataTypes.FLOAT, RealType.INSTANCE)
        .put(DataTypes.DOUBLE, DoubleType.INSTANCE)
        .put(DataTypes.TIMESTAMP, TimestampType.INSTANCE)
        .put(DataTypes.IP, VarCharType.INSTANCE) // postgres has no IP type, so map it to varchar - it matches the client representation
        .put(DataTypes.UNDEFINED, VarCharType.INSTANCE)
        .put(DataTypes.GEO_POINT, PGArray.FLOAT8_ARRAY)
        .put(DataTypes.GEO_SHAPE, JsonType.INSTANCE)
        .put(new ArrayType(DataTypes.BYTE), PGArray.CHAR_ARRAY)
        .put(new ArrayType(DataTypes.SHORT), PGArray.INT2_ARRAY)
        .put(new ArrayType(DataTypes.INTEGER), PGArray.INT4_ARRAY)
        .put(new ArrayType(DataTypes.LONG), PGArray.INT8_ARRAY)
        .put(new ArrayType(DataTypes.FLOAT), PGArray.FLOAT4_ARRAY)
        .put(new ArrayType(DataTypes.DOUBLE), PGArray.FLOAT8_ARRAY)
        .put(new ArrayType(DataTypes.BOOLEAN), PGArray.BOOL_ARRAY)
        .put(new ArrayType(DataTypes.TIMESTAMP), PGArray.TIMESTAMPZ_ARRAY)
        .put(new ArrayType(DataTypes.STRING), PGArray.VARCHAR_ARRAY)
        .put(new ArrayType(DataTypes.GEO_POINT), PGArray.FLOAT8_ARRAY)
        .put(new ArrayType(DataTypes.GEO_SHAPE), PGArray.JSON_ARRAY)
        .put(new ArrayType(DataTypes.OBJECT), JsonType.INSTANCE)
        .build();

    private static final IntObjectMap<DataType> PG_TYPES_TO_CRATE_TYPE = new IntObjectHashMap<>();
    private static final Set<PGType> TYPES;

    static {
        for (Map.Entry<DataType, PGType> e : CRATE_TO_PG_TYPES.entrySet()) {
            int oid = e.getValue().oid();
            // crate string and ip types both map to pg varchar, avoid overwriting the mapping that is first established.
            if (!PG_TYPES_TO_CRATE_TYPE.containsKey(oid)) {
                PG_TYPES_TO_CRATE_TYPE.put(oid, e.getKey());
            }
        }
        PG_TYPES_TO_CRATE_TYPE.put(0, DataTypes.UNDEFINED);
        TYPES = new HashSet<>(CRATE_TO_PG_TYPES.values()); // some pgTypes are used multiple times, de-dup them
    }


    public static Iterable<PGType> pgTypes() {
        return TYPES;
    }

    public static DataType fromOID(int oid) {
        return PG_TYPES_TO_CRATE_TYPE.get(oid);
    }

    public static PGType get(DataType type) {
        if (type instanceof CollectionType) {
            DataType<?> innerType = ((CollectionType) type).innerType();
            if (innerType instanceof CollectionType) {
                // if this is a nested collection stream it as JSON because
                // postgres binary format doesn't support multidimensional arrays with sub-arrays of different length
                // (something like [ [1, 2], [3] ] is not supported)
                return JsonType.INSTANCE;
            }
        }
        PGType pgType = CRATE_TO_PG_TYPES.get(type);
        if (pgType == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "No type mapping from '%s' to pg_type", type.getName()));
        }
        return pgType;
    }
}
