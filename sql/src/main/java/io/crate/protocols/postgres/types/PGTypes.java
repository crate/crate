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
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;
import java.util.Map;

public class PGTypes {

    private static final Map<DataType, PGType> CRATE_TO_PG_TYPES = ImmutableMap.<DataType, PGType>builder()
        .put(DataTypes.STRING, new VarCharType())
        .put(DataTypes.BOOLEAN, new BooleanType())
        .put(DataTypes.OBJECT, new JsonType())
        .put(DataTypes.SHORT, new SmallIntType())
        .put(DataTypes.INTEGER, new IntegerType())
        .put(DataTypes.LONG, new BigIntType())
        .put(DataTypes.FLOAT, new RealType())
        .put(DataTypes.DOUBLE, new DoubleType())
        .put(DataTypes.TIMESTAMP, new TimestampType())
        .put(DataTypes.IP, new VarCharType()) // postgres has no IP type, so map it to varchar - it matches the client representation
        .build();

    private static final IntObjectMap<DataType> PG_TYPES_TO_CRATE_TYPE = new IntObjectHashMap<DataType>()
    {{
        put(0, DataTypes.UNDEFINED);
        put(VarCharType.OID, DataTypes.STRING);
        put(BooleanType.OID, DataTypes.BOOLEAN);
        put(JsonType.OID, DataTypes.OBJECT);
        put(SmallIntType.OID, DataTypes.SHORT);
        put(IntegerType.OID, DataTypes.INTEGER);
        put(BigIntType.OID, DataTypes.LONG);
        put(RealType.OID, DataTypes.FLOAT);
        put(DoubleType.OID, DataTypes.DOUBLE);
    }};

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
