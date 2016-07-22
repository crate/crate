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
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;
import java.util.Map;

public class PGTypes {

    private static final Map<DataType, PGType> CRATE_TO_PG_TYPES = ImmutableMap.<DataType, PGType>builder()
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
        .put(new ArrayType(DataTypes.SHORT), PGArray.INT2_ARRAY)
        .put(new ArrayType(DataTypes.INTEGER), PGArray.INT4_ARRAY)
        .put(new ArrayType(DataTypes.LONG), PGArray.INT8_ARRAY)
        .put(new ArrayType(DataTypes.FLOAT), PGArray.FLOAT4_ARRAY)
        .put(new ArrayType(DataTypes.DOUBLE), PGArray.FLOAT8_ARRAY)
        .put(new ArrayType(DataTypes.BOOLEAN), PGArray.BOOL_ARRAY)
        .put(new ArrayType(DataTypes.TIMESTAMP), PGArray.TIMESTAMPZ_ARRAY)
        .put(new ArrayType(DataTypes.STRING), PGArray.VARCHAR_ARRAY)
        .build();

    public static Iterable<PGType> pgTypes() {
        return CRATE_TO_PG_TYPES.values();
    }

    private static final IntObjectMap<DataType> PG_TYPES_TO_CRATE_TYPE = new IntObjectHashMap<>();
    static {
        for (Map.Entry<DataType, PGType> e : CRATE_TO_PG_TYPES.entrySet()) {
            PG_TYPES_TO_CRATE_TYPE.put(e.getValue().oid(), e.getKey());
        }
        PG_TYPES_TO_CRATE_TYPE.put(0, DataTypes.UNDEFINED);
    }

    public static DataType fromOID(int oid) {
        return PG_TYPES_TO_CRATE_TYPE.get(oid);
    }

    public static PGType get(DataType type) {
        PGType pgType = CRATE_TO_PG_TYPES.get(type);
        if (pgType == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "No type mapping from '%s' to pg_type", type.getName()));
        }
        return pgType;
    }
}
