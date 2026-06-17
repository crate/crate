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

package io.crate.types;

import static java.util.Map.entry;

import java.util.Map;

import org.jspecify.annotations.Nullable;

public class DataTypesBwc {

    private static final Map<String, DataType<?>> MAPPING_NAMES_TO_TYPES = Map.ofEntries(
        entry("date", DataTypes.TIMESTAMPZ),
        entry("string", DataTypes.STRING),
        entry("keyword", DataTypes.STRING),
        entry("text", DataTypes.STRING),
        entry("boolean", DataTypes.BOOLEAN),
        entry("byte", DataTypes.BYTE),
        entry("short", DataTypes.SHORT),
        entry("integer", DataTypes.INTEGER),
        entry("long", DataTypes.LONG),
        entry("float", DataTypes.FLOAT),
        entry("double", DataTypes.DOUBLE),
        entry("ip", DataTypes.IP),
        entry("geo_point", DataTypes.GEO_POINT),
        entry("geo_shape", DataTypes.GEO_SHAPE),
        entry("object", DataTypes.UNTYPED_OBJECT),
        entry("nested", DataTypes.UNTYPED_OBJECT),
        entry("interval", DataTypes.INTERVAL),
        entry(FloatVectorType.INSTANCE_ONE.getName(), FloatVectorType.INSTANCE_ONE),
        entry("undefined", DataTypes.UNDEFINED),
        entry(UUIDType.NAME, UUIDType.INSTANCE)
    );

    private static final Map<Integer, String> TYPE_IDS_TO_MAPPINGS = Map.ofEntries(
        entry(DataTypes.TIMESTAMPZ.id(), "date"),
        entry(DataTypes.TIMESTAMP.id(), "date"),
        entry(DataTypes.STRING.id(), "keyword"),
        entry(DataTypes.CHARACTER.id(), "keyword"),
        entry(DataTypes.BYTE.id(), "byte"),
        entry(DataTypes.BOOLEAN.id(), "boolean"),
        entry(DataTypes.IP.id(), "ip"),
        entry(DataTypes.DOUBLE.id(), "double"),
        entry(DataTypes.FLOAT.id(), "float"),
        entry(DataTypes.SHORT.id(), "short"),
        entry(DataTypes.INTEGER.id(), "integer"),
        entry(DataTypes.LONG.id(), "long"),
        entry(ObjectType.ID, "object"),
        entry(DataTypes.GEO_SHAPE.id(), "geo_shape"),
        entry(DataTypes.GEO_POINT.id(), "geo_point"),
        entry(DataTypes.INTERVAL.id(), "interval"),
        entry(BitStringType.ID, "bit"),
        entry(NumericType.ID, "numeric"),
        entry(FloatVectorType.ID, FloatVectorType.INSTANCE_ONE.getName()),
        entry(UndefinedType.ID, UndefinedType.INSTANCE.getName()),
        entry(UUIDType.ID, UUIDType.NAME)
    );

    /// Returns the ES mapping name for a given DataType id.
    /// Mappings are only used for indices from < 6.0.
    ///
    /// Upgraded indices contain their schema in [org.elasticsearch.cluster.metadata.RelationMetadata]
    /// DataTypes added in >= 6.4 do not have a mapping name.
    @Nullable
    public static String esMappingNameFrom(int typeId) {
        return TYPE_IDS_TO_MAPPINGS.get(typeId);
    }

    /// Returns a DataType by mapping name
    /// Mappings are only used for indices from < 6.0.
    ///
    /// Upgraded indices contain their schema in [org.elasticsearch.cluster.metadata.RelationMetadata]
    /// DataTypes added in >= 6.4 do not have a mapping name.
    @Nullable
    public static DataType<?> ofMappingName(String name) {
        return MAPPING_NAMES_TO_TYPES.get(name);
    }
}
