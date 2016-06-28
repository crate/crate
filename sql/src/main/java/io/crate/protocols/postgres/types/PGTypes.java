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

import java.util.Map;

public class PGTypes {

    public static final Map<DataType, PGType> CRATE_TO_PG_TYPES = ImmutableMap.<DataType, PGType>builder()
        .put(DataTypes.STRING, new VarCharType())
        .put(DataTypes.BOOLEAN, new BooleanType())
        .put(DataTypes.OBJECT, new JsonType())
        .build();

    private static final IntObjectMap<DataType> PG_TYPES_TO_CRATE_TYPE = new IntObjectHashMap<DataType>()
    {{
        put(0, DataTypes.UNDEFINED);
        put(VarCharType.OID, DataTypes.STRING);
        put(BooleanType.OID, DataTypes.BOOLEAN);
        put(JsonType.OID, DataTypes.OBJECT);
    }};

    public static DataType fromOID(int oid) {
        return PG_TYPES_TO_CRATE_TYPE.get(oid);
    }
}
